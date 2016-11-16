package solution;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.StringJoiner;
import java.util.UUID;

public class LocationPublisher {

    public static void main(String[] args) {
        Random random = new Random();
//        long events = 2_000_000;
        long events = 5;

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "location-publisher");
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, CompressionType.LZ4.name);


        String user = UUID.randomUUID().toString(); // "<user-name>"
        double latitude = 42.697708;
        double longitude = 23.321868;

        StringSerializer serializer = new StringSerializer();
        try (KafkaProducer<Void, String> producer = new KafkaProducer<>(properties, NullCodec.instance(), serializer)) {
            for (long n = 0; n < events; n++) {
                long timestamp = System.currentTimeMillis();
                if (random.nextBoolean()) {
                    latitude += random.nextDouble();
                } else {
                    longitude += random.nextDouble();
                }
                String msg = new StringJoiner(",")
                        .add(String.valueOf(timestamp))
                        .add(user)
                        .add(String.valueOf(latitude))
                        .add(String.valueOf(longitude))
                        .toString();
                ProducerRecord<Void, String> record = new ProducerRecord<>("locations", msg);
                producer.send(record);
            }
        }
    }

}