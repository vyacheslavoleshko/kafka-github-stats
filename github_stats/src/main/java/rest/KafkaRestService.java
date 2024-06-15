package rest;

import com.fasterxml.jackson.core.util.JacksonFeature;
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

import javax.inject.Inject;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.StoreQueryParameters.fromNameAndType;

public class KafkaRestService implements GithubAnalyzerApi {

    public static final String REPO_STATS_STORE = "repo-stats-store";
    public static final String UNIQUE_REPOS_STORE = "unique-repos-store";
    public static final String UNIQUE_REPOS_STORE_KEY = "dummyKey";

    private KafkaStreams streams;
    private String thisAppHost;
    private int thisAppPort;

    private final Client client = ClientBuilder.newBuilder().register(JacksonFeature.class).build();

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
            return fetchRepoStatsDataFromOtherInstance(hostInfo, "repository/stats/" + repoName);
        }

        // data is on this instance
        ReadOnlyKeyValueStore<String, RepoStats> repoStatsStore =
                streams.store(fromNameAndType(REPO_STATS_STORE, QueryableStoreTypes.keyValueStore()));
        return repoStatsStore.get(repoName);
    }

    private List<String> fetchUniqueRepoDataFromOtherInstance(HostInfo hostInfo, String path) {
        return client.target(String.format("http://%s:%d/%s", hostInfo.host(), hostInfo.port(), path))
                .request(MediaType.APPLICATION_JSON_TYPE)
                .get(new GenericType<>() {});
    }

    private RepoStats fetchRepoStatsDataFromOtherInstance(HostInfo hostInfo, String path) {
        return client.target(String.format("http://%s:%d/%s", hostInfo.host(), hostInfo.port(), path))
                .request(MediaType.APPLICATION_JSON_TYPE)
                .get(new GenericType<>() {});
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
