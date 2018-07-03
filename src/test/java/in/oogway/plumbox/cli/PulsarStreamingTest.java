package in.oogway.plumbox.cli;

import in.oogway.plumbox.launcher.streaming.*;
import in.oogway.plumbox.SampleTransformation;
import in.oogway.plumbox.cli.testing.LocalTester;
import in.oogway.plumbox.launcher.Ingester;
import in.oogway.plumbox.launcher.Pipeline;
import in.oogway.plumbox.launcher.storage.LauncherStorage;
import in.oogway.plumbox.launcher.storage.LauncherStorageDriver;
import in.oogway.plumbox.launcher.storage.MemoryStorage;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

class PulsarStreamingTest extends LocalTester {
    private String ingesterId;
    private LauncherStorageDriver driver;

    private String[] transformations() {
        return new String[]{SampleTransformation.class.getCanonicalName()};
    }

    // Remove below comment after Pulsar dependency is added.
    // @Test
    void testIngester() throws IllegalAccessException, ClassNotFoundException, InstantiationException,
            JsonProcessingException, PulsarClientException {

        // Start a Pulsar producer asynchronously.
        PulsarProducer pulsarProducer = new PulsarProducer("pulsar-stream");
        Thread producerThread = new Thread(pulsarProducer);
        producerThread.start();

        LauncherStorage<StreamIngester> storage = new LauncherStorage<>(driver);
        StreamIngester ingester = storage.read(ingesterId, StreamIngester.class);

        HashMap<String, Object> args = new HashMap<String, Object>(){{
            put("url", "pulsar://localhost:6650/");
            put("topic", "persistent://public/default/pulsar-stream");
            put("subscription", "test-subscription");
            // To timeout after x milliseconds.
            put("timeout", "40000");
        }};

        ingester.execute(storage, localSession(), args);
        // todo Exit the testcase as soon as the producer exits.
    }

    @BeforeEach
    void setUp() {
        // Initialize a common driver to be used.
        driver = new MemoryStorage();

        Plumbox<Object> pb = new Plumbox<>(driver);

        String source = PulsarSource.class.getCanonicalName();
        String sink = PulsarSink.class.getCanonicalName();

        // Declare a new Pipeline
        Pipeline t = new Pipeline(transformations());
        String tId = pb.declare(t);

        // Save a new Ingester.
        Ingester ig = new Ingester(source, sink, tId);
        ingesterId = pb.declare(ig);
    }
}
