package in.oogway.plumbox.launcher;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;

public class Ingester {
    public String source;
    public String sink;
    public String transformation;

    public Ingester(String source, String sink, String transformation) {
        this.source = source;
        this.sink = sink;
        this.transformation = transformation;
    }

    public void execute(StorageDriver driver, SparkSession ss) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        Source s = (Source) driver.read(source, Source.class);
        Dataset<Row> sourceData = s.load(ss);

        Pipeline tr = (Pipeline)  driver.read(transformation, Pipeline.class);
        ArrayList<Transformer> transformers = tr.inflate();

        // Run all transformations.
        for (Transformer t:
                transformers) {
            sourceData = t.run(sourceData);
        }

        Sink snk = (Sink) driver.read(sink, Sink.class);
        snk.write(sourceData);
    }
}
