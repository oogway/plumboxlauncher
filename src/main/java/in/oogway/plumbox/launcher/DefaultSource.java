package in.oogway.plumbox.launcher;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;

public class DefaultSource implements Source {
    @Override
    public Dataset<Row> load(SparkSession ss, HashMap<String, Object> arguments) {
        return ss.emptyDataFrame();
    }
}
