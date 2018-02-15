package in.oogway.plumbox.cli;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Plumbox {
    private final String CONF_PATH = String.format("%s/%s", System.getenv("HOME"), ".pbconf");

    private YamlHandler yamlHandler = YamlHandler.createYamlHandler();
    private StorageDriver storageDriver;

    private String PLUMBOX_BASE_DIR;
    private String filesystem;

    private Map<String, Object> pbData = new HashMap<>();

    private Plumbox() {
    }

    public static Plumbox createPlumbox() {
        return new Plumbox();
    }

    public void readConfiguration () {
        try {
            Map<String, Object> data = yamlHandler.readYAML(this.CONF_PATH);
            this.PLUMBOX_BASE_DIR = (String) data.get("path");
            this.filesystem = (String) data.get("filesystem");

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        if (this.filesystem.equals("LOCAL")) {
            this.storageDriver = new Localstorage();

        } else if (this.filesystem.equals("REDIS")) {
            this.storageDriver = new RediStorage(this.PLUMBOX_BASE_DIR);
        }
    }

    // init command
    public void initialize (String filesystem, String path) throws IOException {

        // TODO: Validate if path is correct or not
        if (filesystem.equals("LOCAL")) {
            File directory = new File(path);
            if (!directory.exists()) {
                // TODO: Check the output of the directory
                directory.mkdir();
            }
        }

        File f = new File(this.CONF_PATH);

        if(!f.exists()) {
            Map<String, Object> data = new HashMap<>();
            data.put("filesystem", filesystem);
            data.put("path", path);

            try {
                yamlHandler.writeYAML(data, this.CONF_PATH);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public Plumbox setPbData(String key, String value) {
        this.pbData.put(key, value);
        return this;
    }

    public Plumbox setPbData(String key, List<String> value) {
        this.pbData.put(key, value);
        return this;
    }

    public void declareFact() {
        this.readConfiguration();
        this.storageDriver.write(this.PLUMBOX_BASE_DIR, this.pbData, "fact");
    }

    public void declareDimension() {
        this.readConfiguration();
        this.storageDriver.write(this.PLUMBOX_BASE_DIR, this.pbData, "dim");
    }

    public void declareView() {
        this.readConfiguration();
        this.storageDriver.write(this.PLUMBOX_BASE_DIR, this.pbData, "view");
    }

    public void declareTransformation() {
        this.readConfiguration();
        this.storageDriver.write(this.PLUMBOX_BASE_DIR, this.pbData, "trans");
    }

    public void declareIngester() {
        this.readConfiguration();

        if (!this.storageDriver.doesExist((String)this.pbData.get("source"))) {
            //throw new java.lang.Error("Source key does not exists: " + data.get("source"));
            throw new RuntimeException("Source key does not exist: " + this.pbData.get("source"));
        }

        if (!this.storageDriver.doesExist((String)this.pbData.get("transformation"))) {
            //throw new java.lang.Error("Source key does not exists: " + data.get("source"));
            throw new RuntimeException("Transformation key does not exist: " + this.pbData.get("transformation"));
        }

        this.storageDriver.write(this.PLUMBOX_BASE_DIR, this.pbData, "ingest");
    }

    public void listFacts() {
        this.readConfiguration();
        this.storageDriver.read("fact*");
    }

    public void listDimensions() {
        this.readConfiguration();
        this.storageDriver.read("dim*");
    }

    public void listViews() {
        this.readConfiguration();
        this.storageDriver.read("view*");
    }

    public void listTransformations() {
        this.readConfiguration();
        this.storageDriver.read("trans*");
    }

    public void listIngesters() {
        this.readConfiguration();
        this.storageDriver.read("ingest*");
    }
}