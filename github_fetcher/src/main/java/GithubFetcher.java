import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class GithubFetcher {

    private static final String INPUT_TOPIC = "github.accounts";
    private static final String OUTPUT_TOPIC = "github.commits";
    private static final String BOOTSTRAP_SERVERS = "localhost:9091,localhost:9092,localhost:9093";
    private static final Logger log = Logger.getLogger(GithubFetcher.class.getSimpleName());
    private static final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    public static final int BATCH_SIZE = 50;

    private static final ExecutorService messageExecutorService =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 1);
    private static final ExecutorService githubApiExecutorService =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 1);

    private GithubRestClientApi restClient;
    private KafkaProducer<String, String> producer;

    public GithubFetcher(GithubRestClientApi restClient, KafkaProducer<String, String> producer) {
        this.restClient = restClient;
        this.producer = producer;
    }

    public static void main(String[] args) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "github-commits-consumer-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        if (args.length == 0) {
            throw new IllegalStateException("Please, specify Github token as system arg");
        }
        String githubToken = args[0];
        log.info("Received token: " + githubToken);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(INPUT_TOPIC));

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown, closing Consumer and Producer gracefully...");
            messageExecutorService.shutdown();
            githubApiExecutorService.shutdown();
            producer.flush();
            producer.close();
            consumer.close();
        }));

        final GithubFetcher githubFetcher = new GithubFetcher(GithubRestClient.getInstance(githubToken), producer);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            CompletableFuture<Void>[] futures = new CompletableFuture[records.count()];
            int i = 0;
            for (ConsumerRecord<String, String> record : records) {
                futures[i] = CompletableFuture.runAsync(
                                () -> githubFetcher.processFetchRequests(record.value()), messageExecutorService
                        );
                i++;
            }
            CompletableFuture.allOf(futures).join();
            consumer.commitSync();
        }
    }

    @SneakyThrows
    void processFetchRequests(String fetchRequestStr) {
        log.info(String.format("Start processing of FetchRequest: %s", fetchRequestStr));
        List<FetchRequest> fetchRequests = mapper.readValue(fetchRequestStr, new TypeReference<>() {});

        CompletableFuture<Void>[] futures = fetchRequests.stream()
                .peek(fetchRequest -> fetchRequest.setId(UUID.randomUUID()))
                .map(fetchRequest ->
                        CompletableFuture.runAsync(() -> getCommitsAndSend(fetchRequest), githubApiExecutorService))
                .toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(futures).join();
    }

    @SneakyThrows
    private void getCommitsAndSend(FetchRequest fetchRequest) {
        log.info(String.format("%s_%s: Getting commits...", fetchRequest.owner, fetchRequest.repo));
        restClient.processCommitsInBatches(fetchRequest, BATCH_SIZE, this::produceRecords);
    }

     private void produceRecords(List<Commit> commits) {
        commits.forEach(commit -> {
            // Key is composed of unique repo name + unique ID of fetch request. By including ID when calculating
            // statistics downstream we perform GROUP BY for the particular FetchRequest only. If the data for the same
            // owner + repo data has been already fetched before, it will be overridden.
            String key = commit.getRepo() + ":" + commit.getFetchRequestId();
            try {
                producer.send(new ProducerRecord<>(OUTPUT_TOPIC, key, mapper.writeValueAsString(commit)));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Data
    static class FetchRequest {

        private String owner;
        private String repo;
        private LocalDate since;
        private UUID id;
    }

    @Data
    @AllArgsConstructor
    static class Commit {

        String repo;  // in the format of owner:repoName
        String authorName;
        UUID fetchRequestId;
    }

}
