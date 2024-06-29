import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import model.Key;
import model.RepoStats;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import serde.KeySerializer;
import serde.RepoStatsDeserializer;
import serde.TopFiveDeserializer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static rest.KafkaRestService.REPO_STATS_STORE;
import static rest.KafkaRestService.UNIQUE_REPOS_STORE;
import static rest.KafkaRestService.UNIQUE_REPOS_STORE_KEY;


public class GithubAnalyzerTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<Key, String> inputTopic;
    private KeyValueStore<String, String> uniqueRepoStore;
    private KeyValueStore<String, RepoStats> repoStatsStore;
    private final ObjectMapper mapper = new ObjectMapper();
    private final RepoStatsDeserializer repoStatsDeserializer = new RepoStatsDeserializer(new TopFiveDeserializer());

    @Before
    public void setUp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Define the topology
        Topology topology = new GithubAnalyzer().createTopology();
        // Add your stream processing logic here

        testDriver = new TopologyTestDriver(topology, props);
        inputTopic = testDriver.createInputTopic("github.commits", new KeySerializer(), Serdes.String().serializer());
        uniqueRepoStore = testDriver.getKeyValueStore(UNIQUE_REPOS_STORE);
        repoStatsStore = testDriver.getKeyValueStore(REPO_STATS_STORE);
    }

    @After
    public void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    public void singleRepoSingleCommitter() {
        String inputFixture = "/single_repo_single_committer/input.json";
        String expectedRepoNamesFixture = "/single_repo_single_committer/expected_repo_names.txt";
        String expectedRepoStatsFixture = "/single_repo_single_committer/expected_repo_stats.json";
        doAsserts(inputFixture, expectedRepoNamesFixture, expectedRepoStatsFixture);
    }

    @Test
    public void multipleReposMultipleCommitters() {
        String inputFixture = "/multiple_repos_miltiple_committers/input.json";
        String expectedRepoNamesFixture = "/multiple_repos_miltiple_committers/expected_repo_names.txt";
        String expectedRepoStatsFixture = "/multiple_repos_miltiple_committers/expected_repo_stats.json";
        doAsserts(inputFixture, expectedRepoNamesFixture, expectedRepoStatsFixture);
    }

    @Test
    public void newFetchRequestOverridesPrevious() {
        String inputFixture = "/check_new_request_overrides_previous/input.json";
        String expectedRepoNamesFixture = "/check_new_request_overrides_previous/expected_repo_names.txt";
        String expectedRepoStatsFixture = "/check_new_request_overrides_previous/expected_repo_stats.json";
        doAsserts(inputFixture, expectedRepoNamesFixture, expectedRepoStatsFixture);
    }


    private void doAsserts(String inputFixture, String expectedRepoNamesFixture, String expectedRepoStatsFixture) {
        List<Commit> inputMessages = readInputFixture(inputFixture);
        inputMessages.stream()
                .map(this::buildTestRecord)
                .forEach(inputTopic::pipeInput);

        String actualRepos = uniqueRepoStore.get(UNIQUE_REPOS_STORE_KEY);
        assertEquals(readExpectedRepoNamesFixture(expectedRepoNamesFixture), actualRepos);

        List<RepoStats> expectedStats = readExpectedRepoStatsFixture(expectedRepoStatsFixture);
        Map<String, RepoStats> expectedStatsMap = expectedStats
                .stream()
                .collect(Collectors.toMap(RepoStats::getRepo, Function.identity()));
        int repoStatsCount = 0;
        KeyValueIterator<String, RepoStats> iter = repoStatsStore.all();
        while (iter.hasNext()) {
            repoStatsCount++;
            iter.next();
        }
        assertEquals(expectedStatsMap.size(), repoStatsCount);
        expectedStatsMap.forEach((repo, stats) ->
                assertThat(repoStatsStore.get(repo))
                        .usingRecursiveComparison()
                        .ignoringFields("repo")
                        .isEqualTo(stats));
    }

    @SneakyThrows
    private List<Commit> readInputFixture(String fixturePath) {
        String content = readFromFile(fixturePath);
        return mapper.readValue(content, new TypeReference<>() {});
    }

    @SneakyThrows
    private String readExpectedRepoNamesFixture(String fixturePath) {
        return readFromFile(fixturePath);
    }

    @SneakyThrows
    private List<RepoStats> readExpectedRepoStatsFixture(String fixturePath) {
        String content = readFromFile(fixturePath);
        JsonNode jsonNode = mapper.readTree(content);

        List<String> jsonStrings = new ArrayList<>();
        for (JsonNode node : jsonNode) {
            jsonStrings.add(mapper.writeValueAsString(node));
        }
        return jsonStrings
                .stream()
                .map(repoStats -> repoStatsDeserializer.deserialize("", repoStats.getBytes()))
                .collect(Collectors.toList());
    }

    @SneakyThrows
    private String readFromFile(String fixturePath) {
        return IOUtils.toString(
                this.getClass().getResourceAsStream(fixturePath),
                StandardCharsets.UTF_8
        );
    }

    @SneakyThrows
    private TestRecord<Key, String> buildTestRecord(Commit commit) {
        Key key = new Key(commit.getRepo(), commit.getFetchRequestId());
        return new TestRecord<>(key, mapper.writeValueAsString(commit));
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class Commit {

        String repo;  // in the format of owner:repoName
        String authorName;
        UUID fetchRequestId;
    }
}
