import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import model.Contributor;
import model.ContributorWithCount;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import serde.SimpleObjectDeserializer;
import serde.SimpleObjectSerializer;
import topfive.TopFiveContributors;
import topfive.TopFiveDeserializer;
import topfive.TopFiveSerializer;

import java.util.Properties;
import java.util.logging.Logger;

import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE_V2;

public class GithubAnalyzer {

    private static final Logger log = Logger.getLogger(GithubAnalyzer.class.getSimpleName());
    private static final StreamsBuilder builder = new StreamsBuilder();

    public static final String INPUT_TOPIC = "github.commits";
    public static final String OUTPUT_TOPIC = "github.stats";

    private final static Serializer<TopFiveContributors> topFiveSerializer = new TopFiveSerializer();
    private final static Deserializer<TopFiveContributors> topFiveDeserializer = new TopFiveDeserializer();
    private final static Serde<TopFiveContributors> topFiveSerde = Serdes.serdeFrom(topFiveSerializer, topFiveDeserializer);

    private final static Serializer<ContributorWithCount> contributorWithCountSerializer = new SimpleObjectSerializer<>();
    private final static Deserializer<ContributorWithCount> contributorWithCountDeserializer = new SimpleObjectDeserializer<>(ContributorWithCount.class);
    private final static Serde<ContributorWithCount> contributorWithCountSerde = Serdes.serdeFrom(contributorWithCountSerializer, contributorWithCountDeserializer);

    private final static Serializer<Contributor> contributorSerializer = new SimpleObjectSerializer<>();
    private final static Deserializer<Contributor> contributorDeserializer = new SimpleObjectDeserializer<>(Contributor.class);
    private final static Serde<Contributor> contributorSerde = Serdes.serdeFrom(contributorSerializer, contributorDeserializer);


    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        log.info("Starting KafkaStreams...");
        Properties p = new Properties();
        p.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "github-analyzer");
        p.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);
        p.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        p.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        KStream<String, String> commits = builder.stream(INPUT_TOPIC);
        KTable<Contributor, Long> contributorCounts = commits
                .groupBy(GithubAnalyzer::extractContributorFromCommit, Grouped.with(contributorSerde, Serdes.String()))
                .count();
        contributorCounts
                .mapValues((contributor, commitCount) ->
                        new ContributorWithCount(
                                contributor.getRepo(),
                                contributor.getEmail(),
                                commitCount)
                )
                .groupBy((contributor, contributorWithCount) ->
                                KeyValue.pair(
                                        contributorWithCount.getRepo(),
                                        contributorWithCount),
                        Grouped.with(Serdes.String(), contributorWithCountSerde))
                .aggregate(
                        TopFiveContributors::new,
                        (repo, contributorWithCount, agg) -> {
                            agg.add(contributorWithCount);
                            return agg;
                        },
                        (repo, contributorWithCount, agg) -> {
                            agg.remove(contributorWithCount);
                            return agg;
                        },
                        Materialized.<String, TopFiveContributors, KeyValueStore<Bytes, byte[]>>as("top-5")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(topFiveSerde)
                ).toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), topFiveSerde));


        StreamsConfig config = new StreamsConfig(p);
        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, config);

        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Closing KafkaStreams gracefully...");
            streams.close();
        }));

    }

    @SneakyThrows
    private static Contributor extractContributorFromCommit(String repo, String commit) {
        String userEmail = mapper.readTree(commit)
                .path("commit")
                .path("author")
                .path("email").asText();
        return new Contributor(repo, userEmail);
    }

}
