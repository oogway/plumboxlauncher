package in.oogway.plumbox.launcher.streaming;

import in.oogway.plumbox.launcher.Sink;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/* A Pulsar sink implementation of StreamSink. */
public class PulsarSink implements Sink {

    // Saves the data to a CSV file.
    @Override
    public void flush(SparkSession ss, Dataset<Row> output) {
        System.out.println("Reached the sink. Count is " + output.count());
        output.show();
        output.write()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .mode(SaveMode.Append)
                .save("pulsar-output.csv");
    }
}
