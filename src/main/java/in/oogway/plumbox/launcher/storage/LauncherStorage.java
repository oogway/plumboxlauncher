package in.oogway.plumbox.launcher.storage;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.HashMap;
import java.util.Set;
import java.util.UUID;

public class LauncherStorage<T> {
    private final Gson gson;
    private final LauncherStorageDriver driver;

    public LauncherStorage(LauncherStorageDriver driver) {
        gson = new GsonBuilder().create();
        this.driver = driver;
    }

    public T read(final String key, final Class<T> cls) {
        final T data = gson.fromJson(driver.read(key), cls);
        return data;
    }

    public String write(Object data, String prefix) {
        String json = gson.toJson(data);
        String key = String.format("%s-%s", prefix, UUID.randomUUID().toString());
        driver.write(key, json);
        System.out.println("Key %s "+ key);
        return key;
    }

    public HashMap<String, T> readAll(String pattern, Class<T> cls) {
        HashMap<String, T> arr = new HashMap<>();

        Set<String> names = driver.getAllKeys(pattern);
        for (String s : names) {
            T data = gson.fromJson(driver.read(s), cls);
            arr.put(s, data);
        }

        return arr;
    }
}

