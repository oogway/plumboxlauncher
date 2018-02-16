package in.oogway.plumbox.launcher;

import in.oogway.plumbox.config.SparkConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;

public class Source {
    public String path;
    public String driver;
    public String uri;

    public Source(String path, String driver, String uri) {
        this.path = path;
        this.driver = driver;
        this.uri = uri;
    }

    public Dataset<Row> load(SparkSession ss) {
        HashMap<String, String> options = new HashMap<>();
        options.put("uri", uri);

        String driverClass;

        switch (driver) {
            case "jdbc":
                driverClass = "com.mysql.jdbc.Driver";
                options.put("dbtable", path);
            case "mongo":
                driverClass = "com.mongodb.spark.sql.DefaultSource";
                options.put("pipeline", path);
            default:
                options.put("path", path);
                options.put("multiline", "true");
                driverClass = driver;
        }

        Dataset<Row> df = ss.read()
                .format(driverClass)
                .options(options)
                .load();

        return df;
    }
}
