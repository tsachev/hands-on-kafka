package solution;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class NullCodec<T> implements Serializer<T>, Deserializer<T> {

    public static final NullCodec<Object> INSTANCE = new NullCodec<>();

    public NullCodec() {
    }

    @SuppressWarnings("unchecked")
    public static <T> NullCodec<T> instance() {
        return (NullCodec<T>) INSTANCE;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // do nothing
    }

    @Override
    public byte[] serialize(String topic, T data) {
        return null;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        return null;
    }

    @Override
    public void close() {
        // do nothing
    }
}
