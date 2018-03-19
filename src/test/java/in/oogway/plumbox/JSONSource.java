package in.oogway.plumbox;

import in.oogway.plumbox.launcher.Source;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;

public class JSONSource implements Source {
    public Dataset<Row> load(SparkSession sparkSession, HashMap<String, Object> args) {
        HashMap<String, String> options = new HashMap<String, String>() {{
            put("format", "json");
            put("multiline", "true");
            put("path", (String) args.get("path"));
        }};

        DataFrameReader x = sparkSession.read();
        x.format("json");
        x.options(options);
        return x.load();
    }
}
