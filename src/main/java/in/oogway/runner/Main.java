package in.oogway.runner;

import com.esotericsoftware.yamlbeans.YamlException;
import in.oogway.library.config.Config;
import in.oogway.library.ingestor.Ingestor;

import java.io.FileNotFoundException;

public class Main {

    public static void main(String[] args) throws ClassNotFoundException, FileNotFoundException,
            YamlException, InstantiationException, IllegalAccessException {
        String ingestorFileName = "i01.yaml";

        Ingestor ingestorObj = new Ingestor();
        ingestorObj.loadContent(Config.tempPath+"/ingestor/"+ingestorFileName);

    }

}
