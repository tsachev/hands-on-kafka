package solution;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.distance.DistanceCalculator;
import com.spatial4j.core.shape.Point;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;

import static com.spatial4j.core.distance.DistanceUtils.DEG_TO_KM;

public class LocationProcessor {

    private final Properties consumerProperties;
    private final Properties producerProperties;
    private volatile boolean running;
    private Thread thread;

    public LocationProcessor(Properties consumerProperties, Properties producerProperties) {
        this.consumerProperties = consumerProperties;
        this.producerProperties = producerProperties;
    }


    public static void main(String[] args) {

        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "spy");
        consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        consumerProperties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());

        Properties producerProperties = new Properties();
        producerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        LocationProcessor locationProcessor = new LocationProcessor(consumerProperties, producerProperties);
        Runtime.getRuntime().addShutdownHook(new Thread(locationProcessor::stop));

        locationProcessor.start();
    }
    private final Point initial = SpatialContext.GEO.makePoint(42.697708, 23.321868);
    private double maxDistance = 200;


    public void stop() {
        running = false;

        try {
            thread.join(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void start() {
        running = true;
        thread = new Thread(this::run);
        thread.start();
    }


    private void run() {
        DistanceCalculator distanceCalculator = SpatialContext.GEO.getDistCalc();

        IMap<String, String> states = Hazelcast.newHazelcastInstance().getMap("state");
        try (KafkaConsumer<Void, Location> consumer = createConsumer()) {
            consumer.subscribe(Collections.singletonList("locations"));

            Serializer<String> serializer = new StringSerializer();
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties, serializer, serializer)) {
                while (running) {
                    ConsumerRecords<Void, Location> records = consumer.poll(100);
                    records.forEach((record) -> {
                        Location location = record.value();

                        double distance = distanceCalculator.distance(initial, location.getCoordinates()) * DEG_TO_KM;

                        String state = "in";
                        if (distance > maxDistance) {
                            state = "out";
                        }

                        String user = location.getUser();
                        String old = states.put(user, state);
                        if (!Objects.equals(state, old)) {
                            producer.send(new ProducerRecord<>("notifications", user, user + " is " + state + " at " + location.getTimestamp()));
                        }
                    });
                }
            }
        }
    }

    private KafkaConsumer<Void, Location> createConsumer() {
        return new KafkaConsumer<>(consumerProperties, NullCodec.instance(), new LocationDeserializer());
    }

    static class Location {
        private final Instant timestamp;
        private final String user;
        private final Point coordinates;

        public Location(Instant timestamp, String user, Point coordinates) {
            this.timestamp = timestamp;
            this.user = user;
            this.coordinates = coordinates;
        }

        public Instant getTimestamp() {
            return timestamp;
        }

        public String getUser() {
            return user;
        }

        public Point getCoordinates() {
            return coordinates;
        }
    }

    static class LocationDeserializer implements Deserializer<Location> {
        private final StringDeserializer stringDeserializer = new StringDeserializer();
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            stringDeserializer.configure(configs, isKey);
        }

        @Override
        public Location deserialize(String topic, byte[] data) {
            String[] strings = stringDeserializer.deserialize(topic, data).split("[,]");
            String user = strings[1];
            double latitude = Double.parseDouble(strings[2]);
            double longtitude = Double.parseDouble(strings[3]);

            return new Location(Instant.parse(strings[0]), user, SpatialContext.GEO.makePoint(longtitude, latitude));
        }

        @Override
        public void close() {
            stringDeserializer.close();
        }
    }

}