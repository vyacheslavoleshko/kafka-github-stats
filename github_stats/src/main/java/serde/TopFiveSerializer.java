package serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import model.ContributorWithCount;
import model.TopFiveContributors;
import org.apache.kafka.common.serialization.Serializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TopFiveSerializer implements Serializer<TopFiveContributors> {

    public static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No configuration needed
    }

    @SneakyThrows
    @Override
    public byte[] serialize(String topic, TopFiveContributors topFiveContributors) {
        List<ContributorWithCount> contributors = new ArrayList<>();
        for (ContributorWithCount c : topFiveContributors) {
            if (c == null) continue;
            contributors.add(c);
        }
        return mapper.writeValueAsBytes(contributors);
    }

    @Override
    public void close() {
    }

}
