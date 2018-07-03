package in.oogway.plumbox.cli;

import in.oogway.plumbox.JSONSource;
import in.oogway.plumbox.SampleTransformation;
import in.oogway.plumbox.cli.testing.LocalTester;
import in.oogway.plumbox.launcher.DefaultSink;
import in.oogway.plumbox.launcher.Ingester;
import in.oogway.plumbox.launcher.Pipeline;
import in.oogway.plumbox.launcher.storage.LauncherStorage;
import in.oogway.plumbox.launcher.storage.LauncherStorageDriver;
import in.oogway.plumbox.launcher.storage.MemoryStorage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

class PlumboxTest extends LocalTester {
    private String ingesterId;
    private LauncherStorageDriver driver;

    private String[] transformations() {
        return new String[]{SampleTransformation.class.getCanonicalName()};
    }

    // @Test
    void testIngester() {
        LauncherStorage<Ingester> storage = new LauncherStorage<>(driver);
        Ingester ingester = storage.read(ingesterId, Ingester.class);

        HashMap<String, Object> args = new HashMap<String, Object>(){{
            put("path", "src/test/resources/input_source_file.json");
        }};

        try {
            ingester.execute(storage, localSession(), args);
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
        // Initialize a common driver to be used.
        driver = new MemoryStorage();

        Plumbox<Object> pb = new Plumbox<>(driver);

        String source = JSONSource.class.getCanonicalName();
        String sink = DefaultSink.class.getCanonicalName();

        // Declare a new Pipeline
        Pipeline t = new Pipeline(transformations());
        String tId = pb.declare(t);

        // Save a new Ingester.
        Ingester ig = new Ingester(source, sink, tId);
        ingesterId = pb.declare(ig);
    }
}
