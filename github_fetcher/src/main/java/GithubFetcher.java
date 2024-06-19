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
import org.kohsuke.github.GHCommit;
import org.kohsuke.github.GHUser;
import org.kohsuke.github.GitHubBuilder;
import org.kohsuke.github.PagedIterator;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class GithubFetcher {

    private static final String INPUT_TOPIC = "github.accounts";
    private static final String OUTPUT_TOPIC = "github.commits";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final Logger log = Logger.getLogger(GithubFetcher.class.getSimpleName());
    private static final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private static GithubRestClient restClient;

    private static final ExecutorService messageExecutorService =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 1);
    private static final ExecutorService githubApiExecutorService =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 1);
    private static final ExecutorService commitExecutorService =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 1);


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
        try {
            restClient = new GithubRestClient(new GitHubBuilder()
                    .withOAuthToken(githubToken)
                    .build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

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
            commitExecutorService.shutdown();
            producer.flush();
            producer.close();
            consumer.close();
        }));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            CompletableFuture<Void>[] futures = new CompletableFuture[records.count()];
            int i = 0;
            for (ConsumerRecord<String, String> record : records) {
                futures[i] = CompletableFuture.runAsync(
                                () -> processFetchRequests(producer, record.value()), messageExecutorService
                        );
                i++;
            }
            CompletableFuture.allOf(futures).join();
            consumer.commitSync();
        }
    }

    @SneakyThrows
    private static void processFetchRequests(
            KafkaProducer<String, String> producer, String fetchRequestStr
    ) {
        log.info(String.format("Start processing of FetchRequest: %s", fetchRequestStr));
        List<FetchRequest> fetchRequests = mapper.readValue(fetchRequestStr, new TypeReference<>() {});

        CompletableFuture<Void>[] futures = fetchRequests.stream()
                .peek(fetchRequest -> fetchRequest.setId(UUID.randomUUID()))
                .map(fetchRequest ->
                        CompletableFuture.runAsync(() -> getCommitsAndSend(producer, fetchRequest), githubApiExecutorService))
                .toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(futures).join();
    }

    @SneakyThrows
    private static void getCommitsAndSend(KafkaProducer<String, String> producer, FetchRequest fetchRequest) {
        log.info(String.format("%s_%s: Getting commits...", fetchRequest.owner, fetchRequest.repo));
        var iterator = restClient.getCommits(fetchRequest.owner, fetchRequest.repo, fetchRequest.since);
        while (iterator.hasNext()) {
            List<Commit> commits = mapCommits(iterator.nextPage(), fetchRequest);
            log.info(String.format("%s_%s: Fetched %s commits", fetchRequest.owner, fetchRequest.repo, commits.size()));
            produceRecords(producer, commits, fetchRequest.getId());
            log.info(String.format("%s_%s: Produced %s messages", fetchRequest.owner, fetchRequest.repo, commits.size()));
        }
    }

    private static List<Commit> mapCommits(List<GHCommit> commits, FetchRequest req) {
        CompletableFuture<Commit>[] futures = commits.stream()
                .map(commit -> CompletableFuture.supplyAsync(() -> fetchCommitDetails(req.owner, req.repo, commit, req.id), commitExecutorService))
                .toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(futures).join();

        List<Commit> result = new ArrayList<>();
        for (CompletableFuture<Commit> future : futures) {
            result.add(future.join());
        }
        return result;
    }

    @SneakyThrows
    private static Commit fetchCommitDetails(String owner, String repo, GHCommit commit, UUID fetchRequestId) {
        GHUser author = commit.getAuthor();
        String name = "unknown" + UUID.randomUUID();
        if (author != null) name = author.getLogin();
        return new Commit(owner + ":" + repo, name, fetchRequestId);
    }

    private static void produceRecords(
            KafkaProducer<String, String> producer, List<Commit> commits, UUID fetchRequestId) {
        commits.forEach(commit -> {
            // Key is composed of unique repo name + unique ID of fetch request. By including ID when calculating
            // statistics downstream we perform GROUP BY for the particular FetchRequest only. If the data for the same
            // owner + repo data has been already fetched before, it will be overridden.
            String key = commit.getRepo() + ":" + fetchRequestId;
            try {
                producer.send(new ProducerRecord<>(OUTPUT_TOPIC, key, mapper.writeValueAsString(commit)));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Data
    private static class FetchRequest {

        private String owner;
        private String repo;
        private LocalDate since;
        private UUID id;
    }

    @Data
    @AllArgsConstructor
    private static class Commit {

        String repo;  // in the format of owner:repoName
        String authorName;
        UUID fetchRequestId;
    }

}
