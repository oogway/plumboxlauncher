package in.oogway.plumbox.launcher;

import com.mongodb.spark.MongoSpark;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;

public class Source {
    private HashMap<String, String> options;

    public Source(HashMap<String, String> options) {
        this.options = options;
    }

    public Dataset<Row> load(SparkSession ss) {
        //DataFrameReader x = ss.read();
        DataFrameReader x = MongoSpark.read(ss);


        if(options.containsKey("format")) {
            x = x.format(options.get("format"));
        }

        x.options(options);
        return x.load();
    }
}
