package in.oogway.library.ingestor;

import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;
import in.oogway.library.storage.LocalStorage;
import in.oogway.runner.transformer.Transformer;
import org.apache.commons.io.input.CharSequenceReader;
import org.apache.log4j.Logger;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import java.io.IOException;
import java.io.Reader;
import java.util.Map;

public class Ingestor implements LocalStorage {

    public static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(Logger.class);

    public Response loadContent(String path)
            throws IllegalAccessException, InstantiationException, ClassNotFoundException, IOException {
        byte[] bytes = read(path);
        Reader fileReader = getFileReader(bytes);
        Map yamlMap = getYAMLMap(fileReader);

        String type = yamlMap.get("type").toString();
        switch (type){
            case "transformation": // Transformation
                Transformer pbt = getTransformer("in.oogway.runner.transformer.MyTransformerClass");
                pbt.run();
                break;
            case "ingestor": // Ingestor.
                String source = yamlMap.get("source").toString();
                loadContent(source );
                String transformation = yamlMap.get("transformation").toString();
                loadContent(transformation);
                break;
            default: // Source
                // prints source details.
                System.out.println("Printing Source File Details.");
                System.out.println("sid - " + yamlMap.get("sid"));
                System.out.println("source_url - " + yamlMap.get("source_url"));
                System.out.println("driver - " + yamlMap.get("driver"));
                System.out.println("schema - " + yamlMap.get("schema"));
                System.out.println("type - " + yamlMap.get("type"));
                break;
        }
        return Response.status(Status.CREATED).entity("Contents loaded successfully").build();
    }

    public Transformer getTransformer(String transClassPath)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Class act = Class.forName(transClassPath);
        return (Transformer) act.newInstance();
    }

    private Reader getFileReader(byte[] initialArray) throws IOException {
        Reader targetReader = new CharSequenceReader(new String(initialArray));
        targetReader.close();

        return targetReader;
    }

    private Map getYAMLMap(Reader fileReader) throws YamlException {
        YamlReader reader = new YamlReader(fileReader);
        Object object = null;
        object = reader.read();
        return (Map)object;
    }
}
