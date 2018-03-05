package in.oogway.plumbox.launcher;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Source {
    public Dataset<Row> load(SparkSession ss) {
        return ss.read().load();
    }
}