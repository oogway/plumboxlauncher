package in.oogway.library.storage;

import com.esotericsoftware.yamlbeans.YamlException;

import java.io.FileNotFoundException;
import java.util.Map;

public interface StorageDriver {

    Map readYAML(String path) throws FileNotFoundException, YamlException;
}
