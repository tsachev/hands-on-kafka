package demo.producer;

import com.google.common.io.ByteStreams;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public class ThumbnailPublisher {

    private final Producer<String, Thumbnail> producer;

    public ThumbnailPublisher(Producer<String, Thumbnail> producer) {
        this.producer = producer;
    }

    public CompletableFuture<Void> publishThumbnail(String imageId, String title, String url, byte[] content) {
        ProducerRecord<String, Thumbnail> record =
                new ProducerRecord<>("thumbnails", imageId, new Thumbnail(title, url, content));
        CompletableFuture<Void> future = new CompletableFuture<>();
        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                future.complete(null);
            } else {
                future.completeExceptionally(exception);
            }
        });
        return future;
    }


    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "thumbnail-publisher");

        Path path = FileSystems.getDefault().getPath("/run/media/tsachev/Hand-On Kaf/images_2016_08/train", "images.csv");

        try(Producer<String, Thumbnail> producer =
                    new KafkaProducer<>(properties, new StringSerializer(), new ThumbnailSerializer())) {
            ThumbnailPublisher publisher = new ThumbnailPublisher(producer);
            Files.lines(path)
                    .skip(1)
                    .limit(1_000_000)
                    //.parallel()
                    .forEach(line -> {
                        // ImageID,Subset,OriginalURL,OriginalLandingURL,License,AuthorProfileURL,Author,Title,OriginalSize,OriginalMD5,Thumbnail300KURL
                        String[] values = line.split("[,]");

                        String imageId = values[0];
                        String title = values[7];
                        String url = values[2];
                        String thumbnailUrl;

                        byte[] content;
                        try {
                            thumbnailUrl = values[10];
                            content = ByteStreams.toByteArray(new URL(thumbnailUrl).openStream());
                        } catch (Exception e) {
                            return;
                        }
                        publisher.publishThumbnail(imageId, title, url, content);
                    });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
