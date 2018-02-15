package in.oogway.plumbox.cli;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.Map;

public class YamlHandler {
    private DumperOptions options = new DumperOptions();
    private Yaml yaml;

    private YamlHandler() {
        this.options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        this.yaml = new Yaml(this.options);
    }

    public static YamlHandler createYamlHandler() {
        return new YamlHandler();
    }

    public void writeYAML(Map<String, Object> data, String path) throws IOException {
        FileWriter writer = new FileWriter(path);
        yaml.dump(data, writer);
    }

    /*
    public void printYAML (String path) throws FileNotFoundException {
        InputStream input = new FileInputStream(new File(path));
        Object data = this.yaml.load(input);
        System.out.println(data);
    }*/

    public Map<String, Object> readYAML (String path) throws FileNotFoundException {
        InputStream input = new FileInputStream(new File(path));
        Map<String, Object> data = this.yaml.load(input);

        return data;
    }
}
