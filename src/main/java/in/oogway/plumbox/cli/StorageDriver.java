package in.oogway.plumbox.cli;

import java.util.Map;

interface StorageDriverInterface {
    void read(String pattern);
    boolean doesExist(String path);
    void write(String path, Map<String, Object> data, String type);
}

public class StorageDriver implements StorageDriverInterface {

    @Override
    public void read(String pattern) {
    }

    @Override
    public boolean doesExist(String path) {
        return false;
    }

    @Override
    public  void write(String path, Map<String, Object> data, String type) {
    }
}

