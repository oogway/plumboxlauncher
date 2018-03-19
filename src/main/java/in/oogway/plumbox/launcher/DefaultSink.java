package in.oogway.plumbox.launcher;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DefaultSink implements Sink {
    @Override
    public void flush(SparkSession ss, Dataset<Row> output) {
        output.show();
    }
}
