package in.oogway.plumbox.cli;

import org.json.JSONObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class RediStorage extends StorageDriver {
    private Jedis jedis;

    public RediStorage(String host) {
        this.jedis = new Jedis(host);
    }

    public String getKey(String key) {
        try {
            return this.jedis.get(key);

        } catch (JedisConnectionException e) {
            throw new Error("Redis Connection Error: " + e.getMessage());
        }
    }

    public void setKey(String key, String value) {
        try {
            this.jedis.set(key, value);

        } catch (JedisConnectionException e) {
            throw new Error("Redis Connection Error: " + e.getMessage());
        }
    }

    public boolean doesExist(String key) {
        return this.getKey(key) != null;
    }

    public void read(String keypattern) {
        Set<String> names = jedis.keys(keypattern);

        for (String s : names) {
            System.out.println();
            System.out.println(s);
            System.out.println(jedis.get(s));
        }
    }

    public void write(String path, Map<String, Object> data, String type) {
        JSONObject json = new JSONObject(data);

        String uuid = UUID.randomUUID().toString().replace("-", "");
        String key = String.format("%s-%s", type, uuid);

        this.setKey(key, json.toString());
        System.out.println("Redis key: "+ key);

    }
}
