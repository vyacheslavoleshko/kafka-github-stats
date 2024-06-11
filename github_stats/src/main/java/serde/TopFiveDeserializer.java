package serde;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import model.ContributorWithCount;
import model.TopFiveContributors;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.List;
import java.util.Map;

public class TopFiveDeserializer implements Deserializer<TopFiveContributors> {

    public static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No configuration needed
    }

    @SneakyThrows
    @Override
    public TopFiveContributors deserialize(final String s, final byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        List<ContributorWithCount> contributors = mapper.readValue(bytes, new TypeReference<>() {});

        var result = new TopFiveContributors();
        for (var c : contributors) {
            result.add(c);
        }
        return result;
    }

    @Override
    public void close() {
    }
}
