package demo.producer;

import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

class ThumbnailSerializer implements Serializer<Thumbnail> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // do nothing
    }

    @Override
    public byte[] serialize(String topic, Thumbnail data) {
        byte[] title = data.title.getBytes(StandardCharsets.UTF_8);
        byte[] url = data.url.getBytes(StandardCharsets.UTF_8);
        byte[] content = data.content;

        ByteBuffer buffer = ByteBuffer.allocate(2 * Integer.SIZE + title.length + url.length + content.length);

        buffer.putInt(title.length);
        buffer.put(title);

        buffer.putInt(url.length);
        buffer.put(url);

        buffer.put(content);

        return buffer.array();
    }

    @Override
    public void close() {
        //do nothing
    }
}
