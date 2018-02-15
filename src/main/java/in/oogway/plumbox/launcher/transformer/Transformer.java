package in.oogway.plumbox.launcher.transformer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface Transformer {
    Dataset<Row> run(Dataset<Row> rows);
}
