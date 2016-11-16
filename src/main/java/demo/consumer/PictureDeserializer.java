package demo.consumer;

import org.apache.kafka.common.serialization.Deserializer;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

class PictureDeserializer implements Deserializer<Picture> {

    @Override
    public Picture deserialize(String topic, byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int length = buffer.getInt();
        byte[] title = new byte[length];
        buffer.get(title);

        length = buffer.getInt();
        byte[] url = new byte[length];
        buffer.get(url);

        BufferedImage image = null;
        try {
            image = ImageIO.read(new ByteArrayInputStream(buffer.array(), buffer.position(), buffer.remaining()));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new Picture(new String(title, StandardCharsets.UTF_8), image, new String(url, StandardCharsets.UTF_8));
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }
}
