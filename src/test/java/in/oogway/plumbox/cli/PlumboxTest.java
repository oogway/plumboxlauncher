package in.oogway.plumbox.cli;

import in.oogway.plumbox.cli.testing.LocalTester;
import in.oogway.plumbox.launcher.*;
import in.oogway.plumbox.launcher.storage.LauncherStorage;
import in.oogway.plumbox.launcher.storage.MemoryStorage;
import in.oogway.plumbox.launcher.storage.LauncherStorageDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

class PlumboxTest extends LocalTester {
    private String ingesterId;
    private LauncherStorageDriver driver;

    private String[] transformations() {
        return new String[]{
                // Always add a new custom source transformation in the pipeline before other transformations.
                "in.oogway.plumbox.JSONSource",
                "in.oogway.plumbox.SampleTransformation",
                "in.oogway.plumbox.cli.testing.transformation.ShowDF"
        };
    }

    @Test
    void testIngester() {
        LauncherStorage<Ingester> storage = new LauncherStorage<>(driver);
        Ingester ingester = storage.read(ingesterId, Ingester.class);

        try {
            ingester.execute(storage, localSession());
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @BeforeEach
    void setUp() {
        // Attributes for Source.
        System.setProperty("format","json");
        System.setProperty("multiline", "true");
        System.setProperty("path","src/test/resources/input_source_file.json");

        // Initialize a common driver to be used.
        driver = new MemoryStorage();

        Plumbox<Object> pb = new Plumbox<>(driver);

        // Always create a dummy source.
        Source s = new Source(new HashMap<>());
        String sourceId = pb.declare(s);

        // Save a new Sink.
        Sink sk = new Sink("json", "output", "");
        String sinkId = pb.declare(sk);

        // Declare a new Pipeline
        Pipeline t = new Pipeline(String.join(",", transformations()));
        String tId = pb.declare(t);

        // Save a new Ingester.
        Ingester ig = new Ingester(sourceId, sinkId, tId);
        ingesterId = pb.declare(ig);
    }
}
