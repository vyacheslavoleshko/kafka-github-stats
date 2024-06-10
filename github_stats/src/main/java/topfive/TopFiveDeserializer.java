package topfive;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import model.ContributorWithCount;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;

public class TopFiveDeserializer implements Deserializer<TopFiveContributors> {

    public static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No configuration needed
    }

    @Override
    public TopFiveContributors deserialize(final String s, final byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        try(ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes)) {
            byte[] deserialized = inputStream.readAllBytes();
            List<ContributorWithCount> contributors =
                    mapper.readValue(deserialized, new TypeReference<>() {});

            var result = new TopFiveContributors();
            for (var c : contributors) {
                result.add(c);
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
    }
}
