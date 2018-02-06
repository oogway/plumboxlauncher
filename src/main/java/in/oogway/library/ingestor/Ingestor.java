package in.oogway.library.ingestor;

import com.esotericsoftware.yamlbeans.YamlException;
import in.oogway.library.config.Config;
import in.oogway.library.storage.LocalStorage;
import in.oogway.runner.transformer.PBTransformer;

import java.io.FileNotFoundException;
import java.util.Map;

public class Ingestor extends LocalStorage{

    public String[] loadContent(String path)
            throws FileNotFoundException, YamlException, IllegalAccessException,
            ClassNotFoundException, InstantiationException {
        Map yamlMap = super.readYAML(path);
        String type = yamlMap.get("type").toString();
        switch (type){
            case "transformation":
                PBTransformer pbt = getTransformer("in.oogway.runner.transformer.MyTransformerClass");
                pbt.transform();
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

    public PBTransformer getTransformer(String transClassPath)
            throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        Class act = Class.forName(transClassPath);
        return (PBTransformer) act.newInstance();
    }
}
