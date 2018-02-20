package in.oogway.plumbox.launcher;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Sink {
    public String output_format;
    public String path;
    public String uri;

    public Sink(String output_format, String path, String uri) {
        this.output_format = output_format;
        this.path = path;
        this.uri = uri;
    }

    public void write(Dataset<Row> data) {
    }
}
