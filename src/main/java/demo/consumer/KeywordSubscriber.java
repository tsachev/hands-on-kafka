package demo.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import javax.imageio.ImageIO;
import java.awt.AlphaComposite;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.Collections;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.StreamSupport;

public class KeywordSubscriber {

    private static final String IMAGES_FOLDER = "../images";
    private static final AlphaComposite ALPHA = AlphaComposite.getInstance(AlphaComposite.SRC_OVER, 0.5f);

    private final String keyword;
    private final Consumer<String, Picture> consumer;
    private final ImageOutput output;
    private Thread thread;

    private volatile boolean running;

    public KeywordSubscriber(String keyword, Consumer<String, Picture> consumer, ImageOutput output) {
        this.keyword = keyword;
        this.consumer = consumer;
        this.output = output;
    }

    interface ImageOutput {
        OutputStream getOutput(String name);
    }

    private void run() {
        consumer.subscribe(Collections.singletonList("thumbnails"));

        while (running) {
            ConsumerRecords<String, Picture> records = consumer.poll(100);
            StreamSupport.stream(records.spliterator(), false)
                    .filter(this::isMatch)
                    .forEach(this::fetchAndStore);
        }
    }

    private void fetchAndStore(ConsumerRecord<String, Picture> record) {
        Picture picture = record.value();
        try {
            URL url = new URL(picture.url);
            BufferedImage image = ImageIO.read(url);
            BufferedImage combined = combine(picture, image);
            try (OutputStream output = this.output.getOutput(record.key() + ".png")) {
                ImageIO.write(combined, "png", output);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private BufferedImage combine(Picture picture, BufferedImage image) {
        BufferedImage combined =
                new BufferedImage(image.getWidth(), image.getHeight(), BufferedImage.TYPE_INT_ARGB);

        Graphics2D graphics = combined.createGraphics();
        graphics.drawImage(image, 0, 0, null);
        int textY = 0;
        if (picture.thumbnail != null) {
            graphics.setComposite(ALPHA);
            graphics.drawImage(picture.thumbnail, 0, 0, null);
            graphics.setComposite(ALPHA);
            textY = picture.thumbnail.getHeight();
        }
        graphics.drawString(picture.title, 0, textY);
        return combined;
    }

    private boolean isMatch(ConsumerRecord<String, Picture> record) {
        return record.value().title.toLowerCase(Locale.ENGLISH).contains(keyword);
    }

    public void start() {
        running = true;
        thread = new Thread(this::run);
        thread.start();
    }


    public void stop() {
        running = false;

        try {
            thread.join(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) {
        String keyword = args.length == 1 ? args[0] : "cat";

        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, keyword + "-consumer");
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, Picture> consumer =
                     new KafkaConsumer<>(properties, new StringDeserializer(), new PictureDeserializer());

        ImageOutput output = new ImageOutput() {

            final File target = new File(IMAGES_FOLDER + "/" + keyword);

            {
                if (!target.mkdirs() && !target.exists()) {
                    throw new RuntimeException("failed to create " + target);
                }
            }

            @Override
            public OutputStream getOutput(String name) {
                try {
                    return new FileOutputStream(new File(target, name));
                } catch (FileNotFoundException e) {
                    throw new UncheckedIOException(e);
                }
            }
        };

        KeywordSubscriber subscriber = new KeywordSubscriber(keyword, consumer, output);

        subscriber.start();
        Runtime.getRuntime().addShutdownHook(new Thread(()-> {
            subscriber.stop();
            consumer.close();
        }));
    }
}
