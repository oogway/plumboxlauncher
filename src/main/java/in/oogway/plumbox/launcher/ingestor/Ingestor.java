package in.oogway.plumbox.launcher.ingestor;

import com.kpit.warehouse.transformer.Transformer;
import in.oogway.plumbox.launcher.config.Config;
import in.oogway.plumbox.launcher.config.SparkConfig;
import in.oogway.plumbox.launcher.sink.Sink;
import in.oogway.plumbox.launcher.source.Source;
import in.oogway.plumbox.launcher.storage.RedisStorage;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/*
*   @author talina06 on 2/6/18
*/

public class Ingestor implements RedisStorage {

    private String ingestorID;

    public Ingestor(String id) {
        this.ingestorID = id;
        redisServer();
        Config.sparkSession = new SparkConfig(ingestorID);
    }

    /**
     * Reads ingestor yaml, data from source, executes all transformations of dataframe, writes filtered dataset to sink.
     * @throws ClassNotFoundException
     * @throws IOException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public void execute()
            throws ClassNotFoundException, IOException, InstantiationException, IllegalAccessException {

        // Read ingestor yaml from Redis.
        Map ingestor = loadContent(ingestorID);

        // Create a source Object and load source data in a Dataset.
        Source source = new Source(ingestor.get("source").toString());
        Dataset<Row> sourceData = source.load();

        // Get instances of all transformations.
        String transformationId = ingestor.get("transformation").toString();
        Map tf = loadContent(transformationId);
        ArrayList<Transformer> transformers = inflateTransformers((ArrayList) tf.get("classes"));

        // Run all transformations.
        for (Transformer t:
             transformers) {
            System.out.println(t);
            sourceData = t.run(sourceData);
        }

        // Write to sink.
        Sink sink = new Sink();
        sink.write(sourceData, (String)ingestor.get("sink"));

    }

    /**
     * @param transformers
     * @return An arraylist of instances of Transformation classes.
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    private ArrayList<Transformer> inflateTransformers(ArrayList<String> transformers)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        ArrayList<Transformer> transformerObjs = new ArrayList<>();
        for (String className:
                transformers) {
            Class act = Class.forName(className);
            transformerObjs.add((Transformer) act.newInstance());
        }
        return transformerObjs;
    }

}
