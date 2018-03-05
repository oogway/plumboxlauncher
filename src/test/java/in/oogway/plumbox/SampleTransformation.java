package in.oogway.plumbox;

import in.oogway.plumbox.transformer.Transformer;
import org.apache.spark.sql.*;

public class SampleTransformation implements Transformer {
    private Column getCurrentTS() {
        //Current timestamp from machine
        return functions.current_timestamp();
    }

    @Override
    public Dataset<Row> run(SparkSession sparkSession, Dataset<Row> df) {
        return df.withColumn("processed_on", getCurrentTS());
    }
}