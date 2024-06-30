package serde;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.SneakyThrows;
import model.ContributorWithCount;
import model.RepoStats;
import model.TopFiveContributors;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class RepoStatsSerializer implements Serializer<RepoStats> {

    public static final ObjectMapper mapper = new ObjectMapper();
    private final Serializer<TopFiveContributors> serializer;

    public RepoStatsSerializer(Serializer<TopFiveContributors> serializer) {
        this.serializer = serializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No configuration needed
    }

    @SneakyThrows
    @Override
    public byte[] serialize(String topic, RepoStats repoStats) {
        byte[] topFive = serializer.serialize(topic, repoStats.getTopFiveContributors());
        JsonNode topFiveNode = mapper.readTree(topFive);
        JsonNode repoStatsNode = mapper.readTree(mapper.writeValueAsString(repoStats));
        ((ObjectNode) repoStatsNode).put("topFiveContributors", topFiveNode);
        return mapper.writeValueAsBytes(repoStats);
    }

    @Override
    public void close() {
    }

}
