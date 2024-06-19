package serde;

import lombok.SneakyThrows;
import model.Key;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class KeySerializer implements Serializer<Key> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No configuration needed
    }

    @SneakyThrows
    @Override
    public byte[] serialize(String topic, Key key) {
        return (key.getRepo() + ":" + key.getRequestId()).getBytes();
    }

    @Override
    public void close() {
    }
}
