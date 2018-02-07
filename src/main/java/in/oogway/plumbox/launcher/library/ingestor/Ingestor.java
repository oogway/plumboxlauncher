package in.oogway.plumbox.launcher.library.ingestor;

import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;
import in.oogway.plumbox.launcher.library.config.Config;
import in.oogway.plumbox.launcher.library.storage.RedisStorage;
import in.oogway.plumbox.launcher.runner.transformer.Transformer;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.CharSequenceReader;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.util.ArrayList;
import java.util.Map;

public class Ingestor implements RedisStorage {

    private String ingestorID;

    public Ingestor(String id) {
        this.ingestorID = id;
        // connect to Redis server
        redisServer();
    }

    @Override
    public void redisServer() {
        String address = Config.getDirPath("redis_server_address");
        Config.jedis = new Jedis(address);
    }

    public void execute()
            throws ClassNotFoundException, IOException, InstantiationException, IllegalAccessException {

        Map ingestor = loadContent(ingestorID);
        String sourceId = ingestor.get("source").toString();
        Map source = loadContent(sourceId);

        // todo Printing source as of now. Stubs to be added later.
        printData(source);

        String transformationId = ingestor.get("transformation").toString();
        Map tf = loadContent(transformationId);
        ArrayList<Transformer> transformers = inflateTransformers((ArrayList) tf.get("stages"));

        // todo Create a source object.
        for (Transformer t:
             transformers) {
            t.run(); // todo input: Dataframe, output: Dataframe.
        }

        //todo Write to a sink object.
    }


    private Map loadContent(String id)
            throws IllegalAccessException, InstantiationException, ClassNotFoundException, IOException {
        byte[] bytes = read(id);
        Reader fileReader = getFileReader(bytes);
        return getYAMLMap(fileReader);
    }

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

    private Reader getFileReader(byte[] initialArray) throws IOException {
        Reader targetReader = new CharSequenceReader(new String(initialArray));
        targetReader.close();

        return targetReader;
    }

    private Map getYAMLMap(Reader fileReader) throws YamlException {
        YamlReader reader = new YamlReader(fileReader);
        Object object = reader.read();
        return (Map)object;
    }

    private void printData(Map source){
        // prints source details.
        System.out.println("Printing Source File Details.");
        System.out.println("sid - " + source.get("sid"));
        System.out.println("source_url - " + source.get("source_url"));
        System.out.println("driver - " + source.get("driver"));
        System.out.println("schema - " + source.get("schema"));
        System.out.println("type - " + source.get("type"));
    }
}
