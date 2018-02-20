package in.oogway.plumbox;

import in.oogway.plumbox.launcher.Transformer;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class SampleTransformation implements Transformer {
    private Column getCurrentTS() {
        //Current timestamp from machine
        return functions.current_timestamp();
    }

    @Override
    public Dataset<Row> run(Dataset<Row> df) {
        return df.withColumn("processed_on", getCurrentTS());
    }
}

