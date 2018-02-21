package in.oogway.plumbox;

import in.oogway.plumbox.launcher.Transformer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class ActionsTransformation implements Transformer {
    @Override
    public Dataset<Row> run(Dataset<Row> df) {
        return df.withColumn("_id", functions.col("_id.oid")).withColumn("team",
                functions.col("team.oid")).withColumn("user_created",
                functions.col("user_created.oid"));
    }
}