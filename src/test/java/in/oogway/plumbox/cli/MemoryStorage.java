package in.oogway.plumbox.cli;

import in.oogway.plumbox.launcher.StorageDriver;

import java.util.HashMap;
import java.util.UUID;

public class MemoryStorage<T> implements StorageDriver<T> {
    private HashMap<String, Object> store = new HashMap<>();

    @Override
    public HashMap readAll(String pattern, Class cls) {
        return store;
    }

    @Override
    public Object read(String key, Class cls) {
        return store.get(key);
    }

    @Override
    public String write(String path, Object data, String type) {

        String uuid = UUID.randomUUID().toString();
        String key = String.format("%s-%s", type, uuid);

        store.put(key, data);
        return key;
    }
}
