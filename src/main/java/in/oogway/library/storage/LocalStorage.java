package in.oogway.library.storage;

import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;
import in.oogway.library.config.Config;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

public class LocalStorage implements StorageDriver{

    /**
     * @param path YAML file path
     * @return Map object
     * @throws FileNotFoundException
     * @throws YamlException
     */
    @Override
    public Map readYAML(String path) throws FileNotFoundException, YamlException {
        // return new String[0];
        // should have private method which accepts key and returns values as an array of string.
        YamlReader reader = new YamlReader(new FileReader(path));
        Object object = reader.read();
        System.out.println(object);
        Map map = (Map)object;
        return map;
    }
}