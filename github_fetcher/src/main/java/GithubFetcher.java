import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class GithubFetcher {

    private static final String INPUT_TOPIC = "github.accounts";
    private static final String OUTPUT_TOPIC = "github.commits";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final Logger log = Logger.getLogger(GithubFetcher.class.getSimpleName());

    public static void main(String[] args) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "github-commits-consumer-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(INPUT_TOPIC));

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        ExecutorService executorService = Executors.newFixedThreadPool(10);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, closing Consumer and Producer gracefully...");
                producer.flush();
                producer.close();
                consumer.close();
            }
        });

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                CompletableFuture.runAsync(() -> {
                    for (String dummy : Arrays.asList(
                            "sl@gmail.com", "murr@gmail.com", "murr@gmail.com",
                            "sl@gmail.com", "murr@gmail.com", "lilya@gmail.com",
                            "viva@gmail.com", "tim@gmail.com", "test@gmail.com")) {
                        ProducerRecord<String, String> producerRecord = produceRecord(dummy);
                        producer.send(producerRecord);
                    }
                            var rec1 = produceRecord2("sl@gmail.com");
                            var rec2 = produceRecord2("lilya@gmail.com");
                            var rec3 = produceRecord2("sl@gmail.com");
                            producer.send(rec1);
                            producer.send(rec2);
                            producer.send(rec3);
                }, executorService)
                        .whenComplete((result, ex) -> {
                            if (ex != null) {
                                // TODO: exception handling
                                ex.printStackTrace();
                            }
                        });
            }
        }
    }

    @Data
    private static class AccountInfo {

        String name;
    }

    private static ProducerRecord<String, String> produceRecord(String accountInfo) {
        String commit = "  {\n" +
                "    \"commit\": {\n" +
                "      \"author\": {\n" +
                "        \"name\": \"Slava Oleshko\",\n" +
                "        \"email\": \"" + accountInfo + "\",\n" +
                "        \"date\": \"2011-04-15T16:00:49Z\"\n" +
                "           }\n" +
                "       }\n" +
                "  }";
        return new ProducerRecord<>(OUTPUT_TOPIC, "repo", commit);
    }

    private static ProducerRecord<String, String> produceRecord2(String accountInfo) {
        String commit = "  {\n" +
                "    \"commit\": {\n" +
                "      \"author\": {\n" +
                "        \"name\": \"Slava Oleshko\",\n" +
                "        \"email\": \"" + accountInfo + "\",\n" +
                "        \"date\": \"2011-04-15T16:00:49Z\"\n" +
                "           }\n" +
                "       }\n" +
                "  }";
        return new ProducerRecord<>(OUTPUT_TOPIC, "repo2", commit);
    }

    private static String makeRestCall(String value) {
        // Implement the logic to make a REST call based on the value
        // and return the result
        return "result";
    }

//    "[\n" +
//            "  {\n" +
//            "    \"url\": \"https://api.github.com/repos/octocat/Hello-World/commits/6dcb09b5b57875f334f61aebed695e2e4193db5e\",\n" +
//            "    \"sha\": \"6dcb09b5b57875f334f61aebed695e2e4193db5e\",\n" +
//            "    \"node_id\": \"MDY6Q29tbWl0NmRjYjA5YjViNTc4NzVmMzM0ZjYxYWViZWQ2OTVlMmU0MTkzZGI1ZQ==\",\n" +
//            "    \"html_url\": \"https://github.com/octocat/Hello-World/commit/6dcb09b5b57875f334f61aebed695e2e4193db5e\",\n" +
//            "    \"comments_url\": \"https://api.github.com/repos/octocat/Hello-World/commits/6dcb09b5b57875f334f61aebed695e2e4193db5e/comments\",\n" +
//            "    \"commit\": {\n" +
//            "      \"url\": \"https://api.github.com/repos/octocat/Hello-World/git/commits/6dcb09b5b57875f334f61aebed695e2e4193db5e\",\n" +
//            "      \"author\": {\n" +
//            "        \"name\": \"Monalisa Octocat\",\n" +
//            "        \"email\": \"support@github.com\",\n" +
//            "        \"date\": \"2011-04-14T16:00:49Z\"\n" +
//            "      },\n" +
//            "      \"committer\": {\n" +
//            "        \"name\": \"Monalisa Octocat\",\n" +
//            "        \"email\": \"support@github.com\",\n" +
//            "        \"date\": \"2011-04-14T16:00:49Z\"\n" +
//            "      },\n" +
//            "      \"message\": \"Fix all the bugs\",\n" +
//            "      \"tree\": {\n" +
//            "        \"url\": \"https://api.github.com/repos/octocat/Hello-World/tree/6dcb09b5b57875f334f61aebed695e2e4193db5e\",\n" +
//            "        \"sha\": \"6dcb09b5b57875f334f61aebed695e2e4193db5e\"\n" +
//            "      },\n" +
//            "      \"comment_count\": 0,\n" +
//            "      \"verification\": {\n" +
//            "        \"verified\": false,\n" +
//            "        \"reason\": \"unsigned\",\n" +
//            "        \"signature\": null,\n" +
//            "        \"payload\": null\n" +
//            "      }\n" +
//            "    },\n" +
//            "    \"author\": {\n" +
//            "      \"login\": \"octocat\",\n" +
//            "      \"id\": 1,\n" +
//            "      \"node_id\": \"MDQ6VXNlcjE=\",\n" +
//            "      \"avatar_url\": \"https://github.com/images/error/octocat_happy.gif\",\n" +
//            "      \"gravatar_id\": \"\",\n" +
//            "      \"url\": \"https://api.github.com/users/octocat\",\n" +
//            "      \"html_url\": \"https://github.com/octocat\",\n" +
//            "      \"followers_url\": \"https://api.github.com/users/octocat/followers\",\n" +
//            "      \"following_url\": \"https://api.github.com/users/octocat/following{/other_user}\",\n" +
//            "      \"gists_url\": \"https://api.github.com/users/octocat/gists{/gist_id}\",\n" +
//            "      \"starred_url\": \"https://api.github.com/users/octocat/starred{/owner}{/repo}\",\n" +
//            "      \"subscriptions_url\": \"https://api.github.com/users/octocat/subscriptions\",\n" +
//            "      \"organizations_url\": \"https://api.github.com/users/octocat/orgs\",\n" +
//            "      \"repos_url\": \"https://api.github.com/users/octocat/repos\",\n" +
//            "      \"events_url\": \"https://api.github.com/users/octocat/events{/privacy}\",\n" +
//            "      \"received_events_url\": \"https://api.github.com/users/octocat/received_events\",\n" +
//            "      \"type\": \"User\",\n" +
//            "      \"site_admin\": false\n" +
//            "    },\n" +
//            "    \"committer\": {\n" +
//            "      \"login\": \"octocat\",\n" +
//            "      \"id\": 1,\n" +
//            "      \"node_id\": \"MDQ6VXNlcjE=\",\n" +
//            "      \"avatar_url\": \"https://github.com/images/error/octocat_happy.gif\",\n" +
//            "      \"gravatar_id\": \"\",\n" +
//            "      \"url\": \"https://api.github.com/users/octocat\",\n" +
//            "      \"html_url\": \"https://github.com/octocat\",\n" +
//            "      \"followers_url\": \"https://api.github.com/users/octocat/followers\",\n" +
//            "      \"following_url\": \"https://api.github.com/users/octocat/following{/other_user}\",\n" +
//            "      \"gists_url\": \"https://api.github.com/users/octocat/gists{/gist_id}\",\n" +
//            "      \"starred_url\": \"https://api.github.com/users/octocat/starred{/owner}{/repo}\",\n" +
//            "      \"subscriptions_url\": \"https://api.github.com/users/octocat/subscriptions\",\n" +
//            "      \"organizations_url\": \"https://api.github.com/users/octocat/orgs\",\n" +
//            "      \"repos_url\": \"https://api.github.com/users/octocat/repos\",\n" +
//            "      \"events_url\": \"https://api.github.com/users/octocat/events{/privacy}\",\n" +
//            "      \"received_events_url\": \"https://api.github.com/users/octocat/received_events\",\n" +
//            "      \"type\": \"User\",\n" +
//            "      \"site_admin\": false\n" +
//            "    },\n" +
//            "    \"parents\": [\n" +
//            "      {\n" +
//            "        \"url\": \"https://api.github.com/repos/octocat/Hello-World/commits/6dcb09b5b57875f334f61aebed695e2e4193db5e\",\n" +
//            "        \"sha\": \"6dcb09b5b57875f334f61aebed695e2e4193db5e\"\n" +
//            "      }\n" +
//            "    ]\n" +
//            "  }\n" +
//            "]"
}
