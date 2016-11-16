package demo.producer;

import com.google.common.io.ByteStreams;
import com.google.common.primitives.Bytes;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.net.URL;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.*;

public class ThumbnailPublisherTest {
    @Test
    public void publishThumbnail() throws Exception {
        try (MockProducer<String, Thumbnail> producer =
                     new MockProducer<>(true, new StringSerializer(), new ThumbnailSerializer())) {
            ThumbnailPublisher publisher = new ThumbnailPublisher(producer);
            String url = getClass().getResource("/test.jpg").toString();
            byte[] content = ByteStreams.toByteArray(getClass().getResourceAsStream("/test-thumbnail.jpg"));
            CompletableFuture<Void> future = publisher.publishThumbnail("test-image-id", "test", url, content);

            future.join();
            ProducerRecord<String, Thumbnail> record = producer.history().get(0);

            assertEquals("thumbnails", record.topic());
            assertEquals("test-image-id", record.key());
            assertEquals("test", record.value().title);
            assertEquals(url, record.value().url);
            assertArrayEquals(content, record.value().content);
        }
    }

}