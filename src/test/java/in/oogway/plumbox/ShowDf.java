package in.oogway.plumbox;

import in.oogway.plumbox.launcher.Transformer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class ShowDf implements Transformer {
    @Override
    public Dataset<Row> run(Dataset<Row> rows) {
        rows.show();
        return rows;
    }
}
