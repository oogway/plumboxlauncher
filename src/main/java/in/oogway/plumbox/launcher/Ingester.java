package in.oogway.plumbox.launcher;

import in.oogway.plumbox.launcher.storage.LauncherStorage;
import in.oogway.plumbox.transformer.Transformer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;

public class Ingester {
    public String source;
    public String sink;
    public String pipeline;

    public Ingester(String source, String sink, String transformation) {
        this.source = source;
        this.sink = sink;
        this.pipeline = transformation;
    }

    public void execute(LauncherStorage driver, SparkSession ss) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        Source s = (Source) driver.read(source, Source.class);
        Dataset<Row> sourceData = s.load(ss);

        Pipeline tr = (Pipeline) driver.read(pipeline, Pipeline.class);
        ArrayList<Transformer> transformers = tr.inflate();

        // Run all transformations.
        for (Transformer t:
                transformers) {
            sourceData = t.run(ss, sourceData);
        }

        //Sink snk = (Sink) driver.read(sink, Sink.class);
        //snk.write(sourceData);
    }
}
