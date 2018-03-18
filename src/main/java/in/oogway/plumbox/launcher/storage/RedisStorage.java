package in.oogway.plumbox.launcher.storage;

import redis.clients.jedis.Jedis;

import java.util.Set;

public class RedisStorage implements LauncherStorageDriver {
    private Jedis jedis;

    public RedisStorage(String host) {
        jedis = new Jedis(host);
    }

    @Override
    public void write(String key, String json) {
        jedis.set(key, json);
    }

    @Override
    public String read(String key) {
        return jedis.get(key);
    }

    @Override
    public Set<String> getAllKeys(String pattern) {
        String concat = pattern.concat("*");
        return jedis.keys(concat);
    }
}
