package rest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.util.JacksonFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import model.RepoStats;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.jvnet.hk2.annotations.Service;
import serde.RepoStatsDeserializer;
import serde.TopFiveDeserializer;

import javax.inject.Inject;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.StoreQueryParameters.fromNameAndType;

public class KafkaRestService implements GithubAnalyzerApi {

    private static final Logger log = Logger.getLogger(KafkaRestService.class.getSimpleName());

    public static final String REPO_STATS_STORE = "repo-stats-store";
    public static final String UNIQUE_REPOS_STORE = "unique-repos-store";
    public static final String UNIQUE_REPOS_STORE_KEY = "dummyKey";

    private KafkaStreams streams;
    private String thisAppHost;
    private int thisAppPort;

    private final Client client = ClientBuilder.newBuilder().register(JacksonFeature.class).build();
    private final ObjectMapper mapper = new ObjectMapper();
    private final RepoStatsDeserializer deserializer = new RepoStatsDeserializer(new TopFiveDeserializer());

    public KafkaRestService() {
    }

    @Inject
    public KafkaRestService(KafkaStreams streams, String thisAppHost, int thisAppPort) {
        this.streams = streams;
        this.thisAppHost = thisAppHost;
        this.thisAppPort = thisAppPort;
    }

    @Override
    public List<String> getRepositories() {
        KeyQueryMetadata metadata =
                streamsMetadataForStoreAndKey(UNIQUE_REPOS_STORE, UNIQUE_REPOS_STORE_KEY, Serdes.String().serializer());
        HostInfo hostInfo = metadata.activeHost();
        if (!thisHost(hostInfo)) {
            log.info("Requested data is located not at the current instance. Fetching it from " + hostInfo);
            return fetchUniqueRepoDataFromOtherInstance(hostInfo, "repository");
        }

        // data is on this instance
        ReadOnlyKeyValueStore<String, String> uniqueReposStore =
                streams.store(fromNameAndType(UNIQUE_REPOS_STORE, QueryableStoreTypes.keyValueStore()));
        return Arrays.stream(
                uniqueReposStore.get(UNIQUE_REPOS_STORE_KEY).split(",")
        ).collect(Collectors.toList());
    }

    @Override
    public RepoStats getRepositoryStatistics(String repoName) {
        KeyQueryMetadata metadata =
                streamsMetadataForStoreAndKey(REPO_STATS_STORE, repoName, Serdes.String().serializer());
        HostInfo hostInfo = metadata.activeHost();
        if (!thisHost(hostInfo)) {
            log.info("Requested data is located not at the current instance. Fetching it from " + hostInfo);
            return fetchRepoStatsDataFromOtherInstance(hostInfo, "repository/stats/" + repoName);
        }

        // data is on this instance
        ReadOnlyKeyValueStore<String, RepoStats> repoStatsStore =
                streams.store(fromNameAndType(REPO_STATS_STORE, QueryableStoreTypes.keyValueStore()));
        return repoStatsStore.get(repoName);
    }

    @SneakyThrows
    private List<String> fetchUniqueRepoDataFromOtherInstance(HostInfo hostInfo, String path) {
        Response response = client.target(String.format("http://%s:%d/%s", hostInfo.host(), hostInfo.port(), path))
                .request(MediaType.TEXT_PLAIN)
                .accept(MediaType.TEXT_PLAIN)
                .get();

        if (response.getStatus() != 200) {
            String errorMessage = response.readEntity(String.class);
            log.warning("Failed to fetch data from other instance. HTTP error code: " + response.getStatus() + ". Error message: " + errorMessage);
            return new ArrayList<>();
        }

        String responseData = response.readEntity(String.class);
        return mapper.readValue(responseData, new TypeReference<>() {});
    }

    @SneakyThrows
    private RepoStats fetchRepoStatsDataFromOtherInstance(HostInfo hostInfo, String path) {
        Response response = client.target(String.format("http://%s:%d/%s", hostInfo.host(), hostInfo.port(), path))
                .request(MediaType.TEXT_PLAIN)
                .accept(MediaType.TEXT_PLAIN)
                .get();

        if (response.getStatus() != 200) {
            String errorMessage = response.readEntity(String.class);
            log.warning("Failed to fetch data from other instance. HTTP error code: " + response.getStatus() + ". Error message: " + errorMessage);
            return new RepoStats();
        }

        String responseData = response.readEntity(String.class);
        return deserializer.deserialize("", responseData.getBytes());
    }

    private boolean thisHost(HostInfo hostInfo) {
        return hostInfo.host().equalsIgnoreCase(thisAppHost.replace("http://", ""))
                && hostInfo.port() == thisAppPort;
    }

    private <K> KeyQueryMetadata streamsMetadataForStoreAndKey(final String store,
                                                               final K key,
                                                               final Serializer<K> serializer) {
        // Get metadata for the instances of this Kafka Streams application hosting the store and
        // potentially the value for key
        final KeyQueryMetadata metadata = streams.queryMetadataForKey(store, key, serializer);
        if (metadata == null) {
            throw new NotFoundException();
        }

        return metadata;
    }

}
