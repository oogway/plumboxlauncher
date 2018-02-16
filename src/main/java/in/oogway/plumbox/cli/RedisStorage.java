package in.oogway.plumbox.cli;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import in.oogway.plumbox.launcher.StorageDriver;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Set;
import java.util.UUID;

public class RedisStorage<T> implements StorageDriver<T> {
    private final Gson gson;
    private Jedis jedis;

    public RedisStorage(String host) {
        gson = new GsonBuilder().create();
        jedis = new Jedis(host);
    }

    public T read(final String key, final Class<T> cls) {
        final T data = gson.fromJson(jedis.get(key), cls);
        return data;
    }

    public String write(String path, T data, String type) {
        String json = gson.toJson(data);

        String uuid = UUID.randomUUID().toString();
        String key = String.format("%s-%s", type, uuid);

        jedis.set(key, json);
        System.out.println("Redis key: "+ key);
        return json;
    }

    public HashMap<String, T> readAll(String pattern, Class<T> cls) {
        HashMap<String, T> arr = new HashMap<>();

        Set<String> names = jedis.keys(pattern.concat("*"));
        for (String s : names) {
            T data = gson.fromJson(jedis.get(s), cls);
            arr.put(s, data);
        }

        return arr;
    }
}
