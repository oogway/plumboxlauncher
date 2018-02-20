package in.oogway.plumbox.launcher;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface Transformer {
    Dataset<Row> run(Dataset<Row> rows);
}
