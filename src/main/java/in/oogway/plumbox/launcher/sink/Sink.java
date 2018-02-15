package in.oogway.plumbox.launcher.sink;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/*
*   @author talina06 on 2/12/18
*/

public class Sink {
    /**
     * @param data Transformed Dataset
     * @param path Written to sink url.
     */
    public void write(Dataset<Row> data, String path){
        data.write().format("json").option("path",path);
    }
}
