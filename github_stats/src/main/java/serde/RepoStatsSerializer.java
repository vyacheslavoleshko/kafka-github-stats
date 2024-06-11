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

    public static void main(String[] args) {
        RepoStats r = new RepoStats();
        r.setRepo("repository");
        r.setTotalCommits(3);
        r.setTotalCommitters(2);
        TopFiveContributors t = new TopFiveContributors();
        t.add(new ContributorWithCount("repository", "email1@mail", 1L));
        t.add(new ContributorWithCount("repository", "email2@mail", 2L));
        r.setTopFiveContributors(t);

        byte[] serialized = new RepoStatsSerializer(new TopFiveSerializer()).serialize("", r);
        System.out.println(new String(serialized));
        RepoStatsDeserializer deserializer = new RepoStatsDeserializer(new TopFiveDeserializer());
        RepoStats deserialized = deserializer.deserialize("", serialized);
        System.out.println(deserialized);
    }
}
