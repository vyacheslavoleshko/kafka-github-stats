package serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class SimpleObjectDeserializer<T> implements Deserializer<T> {

    private Class<T> clazz;
    public SimpleObjectDeserializer(Class<T> clazz) {
        this.clazz = clazz;
    }

    public static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No configuration needed
    }

    @Override
    public T deserialize(final String s, final byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        try {
            return mapper.readValue(new String(bytes, StandardCharsets.UTF_8), clazz);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
    }
}
