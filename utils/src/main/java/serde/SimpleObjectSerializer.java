package serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class SimpleObjectSerializer<T> implements Serializer<T> {

    public static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No configuration needed
    }

    @Override
    public byte[] serialize(String topic, T contributor) {
        try {
            return mapper.writeValueAsBytes(contributor);
        } catch (IOException e) {
            throw new IllegalStateException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {
    }
}
