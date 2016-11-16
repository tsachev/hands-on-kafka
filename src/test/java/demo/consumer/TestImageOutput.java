package demo.consumer;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

/**
 * A helper class that can be used to implement check images written by the image consumer.
 */
public class TestImageOutput implements KeywordSubscriber.ImageOutput {
    private Map<String, ByteArrayOutputStream> streams = new HashMap<>();

    @Override
    public OutputStream getOutput(String name) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        streams.put(name, stream);
        return stream;
    }

    public Optional<byte[]> getOutputContent(String name) {
        return Optional.ofNullable(streams.get(name)).map(ByteArrayOutputStream::toByteArray);
    }

    public boolean await(String name, long timeout) throws InterruptedException, TimeoutException {
        long deadline = System.currentTimeMillis() + timeout;
        while (!streams.containsKey(name)) {
            if (deadline < System.currentTimeMillis()) {
                return false;
            }
            Thread.sleep(10);
        }
        return true;
    }
}
