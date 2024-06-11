package serde;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.SneakyThrows;
import model.RepoStats;
import model.TopFiveContributors;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class RepoStatsDeserializer implements Deserializer<RepoStats> {

    private static final ObjectMapper mapper = new ObjectMapper();
    private final Deserializer<TopFiveContributors> topFiveDeserializer;

    public RepoStatsDeserializer(Deserializer<TopFiveContributors> topFiveDeserializer) {
        this.topFiveDeserializer = topFiveDeserializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No configuration needed
    }

    @SneakyThrows
    @Override
    public RepoStats deserialize(final String s, final byte[] bytes) {

        if (bytes == null || bytes.length == 0) {
            return null;
        }
        String jsonString = new String(bytes);
        JsonNode topFiveNode = mapper.readValue(jsonString, JsonNode.class);
        byte[] topFive = topFiveNode.get("topFiveContributors").toString().getBytes();
        TopFiveContributors topFiveContributors = topFiveDeserializer.deserialize("", topFive);
        ((ObjectNode) topFiveNode).put("topFiveContributors", (JsonNode) null);
        RepoStats repoStats = mapper.convertValue(topFiveNode, RepoStats.class);
        repoStats.setTopFiveContributors(topFiveContributors);
        return repoStats;
    }

    @Override
    public void close() {
    }
}
