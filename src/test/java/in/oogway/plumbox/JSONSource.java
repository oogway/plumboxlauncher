package in.oogway.plumbox;

import in.oogway.plumbox.transformer.Transformer;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;

public class JSONSource implements Transformer{

    private HashMap<String, String> options;

    @Override
    public Dataset<Row> run(SparkSession sparkSession, Dataset<Row> dataset) {

        options = new HashMap<String, String>(){{
            put("format", System.getProperty("format"));
            put("multiline", System.getProperty("multiline"));
            put("path", System.getProperty("path"));
        }};

        DataFrameReader x = sparkSession.read();

        if(options.containsKey("format")) {
            x = x.format(options.get("format"));
        }

        x.options(options);
        return x.load();
    }
}
