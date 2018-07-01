package in.oogway.plumbox.launcher.streaming;

import in.oogway.plumbox.launcher.Ingester;
import in.oogway.plumbox.launcher.Pipeline;
import in.oogway.plumbox.launcher.Sink;
import in.oogway.plumbox.launcher.storage.LauncherStorage;
import in.oogway.plumbox.transformer.Transformer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.HashMap;

public class StreamIngester extends Ingester {
    public StreamIngester(String source, String sink, String transformation) {
        super(source, sink, transformation);
    }

    @Override
    public void execute(LauncherStorage driver, SparkSession ss, HashMap<String, Object> args)
            throws IllegalAccessException, InstantiationException, ClassNotFoundException {

        // Get the instances of the SourceStream and the Sink.
        StreamSource<byte[]> source = (StreamSource) getSource(getSourceID());
        Sink sink =  getSink(getSinkID());


        // Start a new Java Streaming Context from the Spark Session.
        JavaStreamingContext jssc = new JavaStreamingContext(JavaSparkContext.fromSparkContext(ss.sparkContext()), Durations.seconds(5));

        // Load the source stream.
        JavaReceiverInputDStream<byte[]> dStream = source.load(jssc, args);

        // Get the transformation class instances.
        Pipeline pipe = (Pipeline) driver.read(getPipelineID(), Pipeline.class);
        Transformer[] transformers = pipe.inflate();

        // Run the pipeline for every RDD in the DStream.
        dStream.foreachRDD( (VoidFunction<JavaRDD<byte[]>>) rdd -> {

            // Convert the JavaRDD to a Dataset.
            JavaRDD<String> strRdd = rdd.map((Function<byte[], String>) cnt -> {
                return new String(cnt, "UTF-8");
            });

            // This will infer the schema automatically.
            // As the output of a Pulsar producer is always a JSON String.
            Dataset<Row> rows = ss.read().json(strRdd);

            // Run all transformations. Incrementally.
            for (Transformer t:
                    transformers) {
                rows = t.run(ss, rows);
            }

            // Save the transformed Dataset to the sink.
            sink.flush(ss, rows);

        });

        try {
            source.startStreaming(jssc,
                    Long.parseLong(String.valueOf(args.getOrDefault("timeout", 0))));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
