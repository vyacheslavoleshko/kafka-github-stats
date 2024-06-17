import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import lombok.SneakyThrows;
import model.Contributor;
import model.ContributorWithCount;
import model.RepoStats;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import rest.GithubAnalyzerRestService;
import rest.KafkaRestService;
import rest.WebServer;
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
import serde.SimpleObjectDeserializer;
import serde.SimpleObjectSerializer;
import model.TopFiveContributors;
import serde.TopFiveDeserializer;
import serde.TopFiveSerializer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;

import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE_V2;

public class GithubAnalyzer {

    private static final Logger log = Logger.getLogger(GithubAnalyzer.class.getSimpleName());
    private static final StreamsBuilder builder = new StreamsBuilder();

    public static final String INPUT_TOPIC = "github.commits";
    public static final String OUTPUT_TOPIC = "github.stats";
    public static final String REPO_STATS_STORE = "repo-stats-store";
    public static final String UNIQUE_REPOS_STORE = "unique-repos-store";
    public static final String UNIQUE_REPOS_STORE_KEY = "dummyKey";
    // TODO: externalize
    public static final String API_HOST = "http://localhost";
    public static final int API_PORT = 8070;

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
        log.info("Starting KafkaStreams...");
        Properties p = new Properties();
        p.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "github-analyzer");
        p.setProperty(StreamsConfig.APPLICATION_SERVER_CONFIG,  API_HOST + ":" + API_PORT);
        p.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);
        p.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        p.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        KStream<String, String> commits = builder.stream(INPUT_TOPIC);
        KTable<String, Long> totalCommits = commits.groupByKey().count();

        totalCommits.toStream()
                // Put all data into a single partition
                .map((repoName, commitCount) -> new KeyValue<>(UNIQUE_REPOS_STORE_KEY, repoName))
                .groupByKey()
                .reduce((oldV, newV) -> {
                    Set<String> uniqueRepoNames = new LinkedHashSet<>(Arrays.asList(oldV.split(",")));
                    uniqueRepoNames.add(newV);
                    var uniqueRepos = uniqueRepoNames.stream().reduce((a, b) -> a + "," + b);
                    return uniqueRepos.orElse("");
                }, Materialized.as(UNIQUE_REPOS_STORE));

        KTable<Contributor, Long> contributorCounts = commits
                .groupBy(GithubAnalyzer::extractContributorFromCommit, Grouped.with(contributorSerde, Serdes.String()))
                .count();
        KTable<String, Long> totalUniqueCommitters =
                contributorCounts
                        .groupBy((contributor, commitCnt) -> KeyValue.pair(contributor.getRepo(), commitCnt),
                                Grouped.with(Serdes.String(), Serdes.Long()))
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
                        RepoStats::new,
                        (repo, contributorWithCount, agg) -> {
                            agg.getTopFiveContributors().add(contributorWithCount);
                            return agg;
                        },
                        (repo, contributorWithCount, agg) -> {
                            agg.getTopFiveContributors().remove(contributorWithCount);
                            return agg;
                        }, Materialized.with(Serdes.String(), repoStatsSerde)
                )
                .join(totalUniqueCommitters, (r, committersCnt) -> {
                    r.setTotalCommitters(committersCnt);
                    return r;
                }, Materialized.with(Serdes.String(), repoStatsSerde))
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

        ResourceConfig resourceConfig = new ResourceConfig();
        resourceConfig.register(GithubAnalyzerRestService.class);
        resourceConfig.register(JacksonJaxbJsonProvider.class);
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


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Closing KafkaStreams gracefully...");
            streams.close();
            try {
                webServer.stopWebServer();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }));

    }

    @SneakyThrows
    private static Contributor extractContributorFromCommit(String repo, String commit) {
        String authorName = mapper.readTree(commit)
                .path("authorName").asText();
        return new Contributor(repo, authorName);
    }

}
