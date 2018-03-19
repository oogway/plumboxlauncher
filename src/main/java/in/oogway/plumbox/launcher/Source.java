package in.oogway.plumbox.launcher;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;

public interface Source {
    Dataset<Row> load(SparkSession ss, HashMap<String, Object> arguments);

    default Object getLastWatermark(Dataset<Row> batch) {
        return null;
    }

    default void setWatermark(Object mark)  {
    }
}

