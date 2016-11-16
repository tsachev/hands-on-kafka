package demo.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.util.Collections;
import java.util.Optional;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class KeywordSubscriberTest {

    @Test
    public void test() throws Exception {
        BufferedImage thumbnail = ImageIO.read(getClass().getResourceAsStream("/test-thumbnail.jpg"));
        String url = getClass().getResource("/test-thumbnail.jpg").toString();

        try (MockConsumer<String, Picture> consumer = new MockConsumer<>(OffsetResetStrategy.LATEST)) {
            consumer.schedulePollTask(() -> {
                consumer.rebalance(Collections.singleton(new TopicPartition("thumbnails", 0)));
                // Mock consumers need to seek manually since they cannot automatically reset offsets
                consumer.updateEndOffsets(Collections.singletonMap(new TopicPartition("thumbnails", 0), 0L));
            });
            consumer.schedulePollTask(() -> {
                Picture picture = new Picture("test image", thumbnail, url);
                ConsumerRecord<String, Picture> thumbnails =
                        new ConsumerRecord<>("thumbnails", 0, 1L, "test-image-id", picture);
                consumer.addRecord(thumbnails);
            });
            TestImageOutput output = new TestImageOutput();
            KeywordSubscriber subscriber = new KeywordSubscriber("test", consumer, output);

            subscriber.start();

            assertTrue(output.await("test-image-id.png", 1000));

            Optional<byte[]> maybeContent = output.getOutputContent("test-image-id.png");
            assertTrue(maybeContent.isPresent());

            subscriber.stop();
        }

    }

}
