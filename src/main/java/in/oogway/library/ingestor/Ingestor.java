package in.oogway.library.ingestor;

import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;
import in.oogway.library.config.Config;
import in.oogway.library.storage.LocalStorage;
import in.oogway.runner.transformer.Transformer;
import org.apache.commons.io.input.CharSequenceReader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.util.Map;

public class Ingestor implements LocalStorage {


    public String[] loadContent(String path)
            throws FileNotFoundException, YamlException, IllegalAccessException,
            ClassNotFoundException, InstantiationException {

        byte[] bytes = read(path);
        Reader fileReader = getReader(bytes);
        Map yamlMap = getYAMLMap(fileReader);

        String type = yamlMap.get("type").toString();
        switch (type){
            case "transformation":
                Transformer pbt = getTransformer("in.oogway.runner.transformer.MyTransformerClass");
                pbt.run();
                break;
            case "ingestor": // Ingestor.
                String source = yamlMap.get("source").toString();
                loadContent(Config.tempPath+"/source/"+source+".yaml");
                String transformation = yamlMap.get("transformation").toString();
                loadContent(Config.tempPath+"/transformer/"+transformation+".yaml");
                break;
            default: // Source
                // print source details.
                System.out.println("sid - " + yamlMap.get("sid"));
                System.out.println("source_url - " + yamlMap.get("source_url"));
                System.out.println("driver - " + yamlMap.get("driver"));
                System.out.println("schema - " + yamlMap.get("schema"));
                System.out.println("type - " + yamlMap.get("type"));
                break;
        }
        return null;
    }

    public Transformer getTransformer(String transClassPath)
            throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        Class act = Class.forName(transClassPath);
        return (Transformer) act.newInstance();
    }

    private Reader getReader(byte[] initialArray){
        Reader targetReader = new CharSequenceReader(new String(initialArray));
        try {
            targetReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return targetReader;
    }

    private Map getYAMLMap(Reader fileReader){

        YamlReader reader = new YamlReader(fileReader);
        Object object = null;
        try {
            object = reader.read();
        } catch (YamlException e) {
            e.printStackTrace();
        }
        System.out.println(object);
        return (Map)object;
    }
}
