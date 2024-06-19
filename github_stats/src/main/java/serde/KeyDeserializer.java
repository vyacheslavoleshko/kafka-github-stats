package serde;

import lombok.SneakyThrows;
import model.Key;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;
import java.util.UUID;

public class KeyDeserializer implements Deserializer<Key> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No configuration needed
    }

    @SneakyThrows
    @Override
    public Key deserialize(final String s, final byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        String[] parts = new String(bytes).split(":");
        return new Key(parts[0] + ":" + parts[1], UUID.fromString(parts[2]));
    }

    @Override
    public void close() {
    }
}
