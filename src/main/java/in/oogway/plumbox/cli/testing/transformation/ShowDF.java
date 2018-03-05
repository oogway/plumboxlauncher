package in.oogway.plumbox.cli.testing.transformation;

import in.oogway.plumbox.transformer.Transformer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by jaideep Khandelwal<jaideep@oogway.in> on 22/02/18.
 */
public class ShowDF implements Transformer {
    @Override
    public Dataset<Row> run(SparkSession ss, Dataset<Row> rows) {
        rows.show();
        return rows;
    }
}