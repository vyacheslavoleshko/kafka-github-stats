import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import model.Contributor;
import model.ContributorWithCount;
import model.Key;
import model.RepoStats;
import org.apache.kafka.streams.kstream.Consumed;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import rest.GithubAnalyzerRestService;
import rest.KafkaRestService;
import rest.WebServer;
import serde.KeyDeserializer;
import serde.KeySerializer;
import serde.RepoStatsDeserializer;
import serde.RepoStatsSerializer;
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
import model.TopFiveContributors;
import serde.TopFiveDeserializer;
import serde.TopFiveSerializer;
import utils.src.main.java.serde.SimpleObjectDeserializer;
import utils.src.main.java.serde.SimpleObjectSerializer;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.ext.Provider;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE_V2;

public class GithubAnalyzer {

    private static final Logger log = Logger.getLogger(GithubAnalyzer.class.getSimpleName());
    private static final StreamsBuilder builder = new StreamsBuilder();

    public static final String INPUT_TOPIC = "github.commits";
    public static final String OUTPUT_TOPIC = "github.stats";
    public static final String REPO_STATS_STORE = "repo-stats-store";
    public static final String UNIQUE_REPOS_STORE = "unique-repos-store";
    public static final String UNIQUE_REPOS_STORE_KEY = "dummyKey";

    public static String API_HOST = "";
    public static int API_PORT = -1;
    public static String APPLICATION_ID = "";

    private final static Serializer<Key> keySerializer = new KeySerializer();
    private final static Deserializer<Key> keyDeserializer = new KeyDeserializer();
    private final static Serde<Key> keySerde = Serdes.serdeFrom(keySerializer, keyDeserializer);

    private final static Serializer<TopFiveContributors> topFiveSerializer = new TopFiveSerializer();
    private final static Deserializer<TopFiveContributors> topFiveDeserializer = new TopFiveDeserializer();

    private final static Serializer<RepoStats> repoStatsSerializer = new RepoStatsSerializer(topFiveSerializer);
    private final static Deserializer<RepoStats> repoStatsDeserializer = new RepoStatsDeserializer(topFiveDeserializer);
    private final static Serde<RepoStats> repoStatsSerde = Serdes.serdeFrom(repoStatsSerializer, repoStatsDeserializer);

    private final static Serializer<ContributorWithCount> contributorWithCountSerializer = new SimpleObjectSerializer<>();
    private final static Deserializer<ContributorWithCount> contributorWithCountDeserializer = new SimpleObjectDeserializer<>(ContributorWithCount.class);
    private final static Serde<ContributorWithCount> contributorWithCountSerde = Serdes.serdeFrom(contributorWithCountSerializer, contributorWithCountDeserializer);

    private final static Serializer<Contributor> contributorSerializer = new SimpleObjectSerializer<>();
    private final static Deserializer<Contributor> contributorDeserializer = new SimpleObjectDeserializer<>(Contributor.class);
    private final static Serde<Contributor> contributorSerde = Serdes.serdeFrom(contributorSerializer, contributorDeserializer);

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        API_HOST = args[0];
        API_PORT = Integer.parseInt(args[1]);
        APPLICATION_ID = args[2];

        log.info("Starting KafkaStreams...");
        Properties p = new Properties();
        p.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        p.setProperty(StreamsConfig.APPLICATION_SERVER_CONFIG,  API_HOST + ":" + API_PORT);
        p.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091,localhost:9092,localhost:9093");
        p.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);
        p.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        p.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        KStream<Key, String> commits = builder.stream(INPUT_TOPIC, Consumed.with(keySerde, Serdes.String()));
        KTable<String, Long> totalCommits =
                commits.groupByKey()
                    .count()
                    .toStream()
                    .selectKey((k, v) -> k.getRepo())
                    .toTable();

        totalCommits.toStream()
                // Put all data into a single partition. This is needed to avoid querying all Kafka Streams app
                // instances to gather the entire state.
                .map((repoName, commitCount) -> new KeyValue<>(UNIQUE_REPOS_STORE_KEY, repoName))
                .groupByKey()
                .reduce((oldV, newV) -> {
                    Set<String> uniqueRepoNames = new LinkedHashSet<>(Arrays.asList(oldV.split(",")));
                    uniqueRepoNames.add(newV);
                    var uniqueRepos = uniqueRepoNames.stream().reduce((a, b) -> a + "," + b);
                    return uniqueRepos.orElse("");
                }, Materialized.as(UNIQUE_REPOS_STORE));

        KTable<Contributor, Long> contributorCount = commits
                .groupBy(GithubAnalyzer::extractContributorFromCommit, Grouped.with(contributorSerde, Serdes.String()))
                .count();
        KTable<String, Long> totalUniqueCommitters =
                contributorCount
                        .groupBy((k, v) -> KeyValue.pair(k.getKey(), v),
                                Grouped.with(keySerde, Serdes.Long()))
                        .count()
                        .toStream()
                        .selectKey((k, v) -> k.getRepo())
                        .toTable();

        contributorCount
                .mapValues((contributor, commitCount) ->
                        new ContributorWithCount(
                                contributor.getRepo(),
                                contributor.getLogin(),
                                commitCount)
                )
                .groupBy((contributor, contributorWithCount) ->
                                KeyValue.pair(
                                        contributor.getKey(),
                                        contributorWithCount),
                        Grouped.with(keySerde, contributorWithCountSerde))
                .aggregate(
                        RepoStats::new,
                        (key, contributorWithCount, agg) -> {
                            agg.getTopFiveContributors().add(contributorWithCount);
                            return agg;
                        },
                        (key, contributorWithCount, agg) -> {
                            agg.getTopFiveContributors().remove(contributorWithCount);
                            return agg;
                        }, Materialized.with(keySerde, repoStatsSerde)
                ).toStream()
                .selectKey((k, v) -> k.getRepo())
                .toTable()
                .join(totalUniqueCommitters, (r, committersCnt) -> {
                    r.setTotalCommitters(committersCnt);
                    return r;
                })
                .join(totalCommits, (r, commitsCnt) -> {
                    r.setTotalCommits(commitsCnt);
                    return r;
                }, Materialized.<String, RepoStats, KeyValueStore<Bytes, byte[]>>as(REPO_STATS_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(repoStatsSerde))
                .toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), repoStatsSerde));


        StreamsConfig config = new StreamsConfig(p);
        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, config);
        streams.cleanUp();
        streams.start();

        WebServer webServer = new WebServer(API_HOST, API_PORT);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Closing KafkaStreams gracefully...");
            streams.close();
            try {
                webServer.stopWebServer();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }));

        ResourceConfig resourceConfig = new ResourceConfig();
        resourceConfig.register(GithubAnalyzerRestService.class);
        resourceConfig.register(JacksonJaxbJsonProvider.class);
        resourceConfig.register(new CORSFilter());
        resourceConfig.register(new AbstractBinder() {
            @Override
            protected void configure() {
                bind(KafkaRestService.class).to(KafkaRestService.class);
                bind(streams).to(KafkaStreams.class);
                bind(API_PORT).to(Integer.class);
                bind(API_HOST).to(String.class);
            }
        });
        webServer.startWebServer(resourceConfig);

    }

    @Provider
    public static class CORSFilter implements ContainerResponseFilter {

        @Override
        public void filter(final ContainerRequestContext requestContext,
                           final ContainerResponseContext cres) {
            cres.getHeaders().add("Access-Control-Allow-Origin", "*");
            cres.getHeaders().add("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD");
            cres.getHeaders().add("Access-Control-Allow-Credentials", "true");
            cres.getHeaders().add("Access-Control-Allow-Headers", "Access-Control-Allow-Headers, Origin,Accept, X-Requested-With, Content-Type, Access-Control-Request-Method, Access-Control-Request-Headers");
        }
    }

    @SneakyThrows
    private static Contributor extractContributorFromCommit(Key key, String commit) {
        JsonNode commitNode = mapper.readTree(commit);
        String authorName = commitNode
                .path("authorName").asText();
        UUID fetchRequestId = UUID.fromString(commitNode
                .path("fetchRequestId").asText());
        return new Contributor(key.getRepo(), authorName, fetchRequestId);
    }

}
