package in.oogway.plumbox.launcher;

import java.util.HashMap;

public interface StorageDriver<T> {
    HashMap<String, T> readAll(String pattern, Class<T> cls);
    T read(String key, Class<T> cls);
    String write(String path, T data, String type);
}

