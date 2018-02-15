package in.oogway.plumbox.cli;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

public class Localstorage extends StorageDriver {
    YamlHandler yamlHandler = YamlHandler.createYamlHandler();

    public void read (String pattern) {
        System.out.println("This is localStorage");
    }

    public boolean doesExist(String key) {
        // TODO: Need to implement
        return false;
    }

    public void write (String path, Map<String, Object> data, String type) {
        System.out.println("This is localStorage write");

        // Check if the directory exists for given type
        String dirpath = String.format("%s/%s", path, type);

        File directory = new File(dirpath);
        if (!directory.exists()){
            directory.mkdir();
        }

        String name = data.get("input_table") == null ? UUID.randomUUID().toString().replace("-", "") : (String)data.get("input_table");
        String filepath = String.format("%s/%s/%s.yaml", path, type, name);

        try {
            yamlHandler.writeYAML(data, filepath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
