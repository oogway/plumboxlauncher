package in.oogway.plumbox.launcher;

import in.oogway.plumbox.launcher.storage.LauncherStorage;
import in.oogway.plumbox.transformer.Transformer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;

public class Ingester {
    public String source;
    public String sink;
    public String pipeline;

    public Ingester(String source, String sink, String transformation) {
        this.source = source;
        this.sink = sink;
        this.pipeline = transformation;
    }

    private Source getSource(String name) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        if (name == null || name.equals("")) {
            return new DefaultSource();
        }

        Class act = Class.forName(name.trim());
        return (Source) act.newInstance();
    }

    private Sink getSink(String name) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        if (name == null || name.equals("")) {
            return new DefaultSink();
        }

        Class act = Class.forName(name.trim());
        return (Sink) act.newInstance();
    }

    public void execute(LauncherStorage driver, SparkSession ss) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        this.execute(driver, ss, new HashMap<>());
    }

    public void execute(LauncherStorage driver, SparkSession ss, HashMap<String, Object> args) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        Source s = getSource(source);
        Sink sk = getSink(sink);
        Pipeline pipe = (Pipeline) driver.read(pipeline, Pipeline.class);
        Transformer[] transformers = pipe.inflate();

        Dataset<Row> rows = s.load(ss, args);
        Object watermark = s.getLastWatermark(rows);

        // Run all transformations. Incrementally.
        for (Transformer t:
                transformers) {
            rows = t.run(ss, rows);
        }

        // Flush the dataset to the Sink.
        sk.flush(ss, rows);

        // If a watermark was returned by the source, write it back.
        if (watermark != null) {
            s.setWatermark(watermark);
        }
    }
}
