package in.oogway.plumbox.launcher.storage;

import java.util.Set;

public interface LauncherStorageDriver {
    void write(String key, String json);
    String read(String s);
    Set<String> getAllKeys(String pattern);
}

