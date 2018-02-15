package in.oogway.plumbox.launcher.source;

/*
*   @author talina06 on 2/12/18
*/

import in.oogway.plumbox.launcher.config.Config;
import in.oogway.plumbox.launcher.storage.JSONStorage;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.io.Reader;
import java.util.Map;

public class Source implements JSONStorage {

    private String sourceID;

    public Source(String id){
        this.sourceID = id;
    }

    /**
     * @return Dataset of the content loaded from source_url
     * @throws IOException
     */
    public Dataset<Row> load() throws IOException {
        byte[] bytes = read(sourceID);
        Reader fileReader = getFileReader(bytes);
        Map sourceMap = getJSONMap(fileReader);
        return Config.sparkSession.getSession().read().option("multiLine", true).json((String)sourceMap.get("source"));
    }
}
