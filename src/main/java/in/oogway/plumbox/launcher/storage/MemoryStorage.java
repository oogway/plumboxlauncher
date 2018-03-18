package in.oogway.plumbox.launcher.storage;

import java.util.HashMap;
import java.util.Set;

public class MemoryStorage implements LauncherStorageDriver {
    private HashMap<String, String> store = new HashMap<>();

    @Override
    public void write(String key, String data) {
        store.put(key, data);
    }

    @Override
    public String read(String s) {
        return store.get(s);
    }

    @Override
    public Set<String> getAllKeys(String pattern) {
        return store.keySet();
    }
}
