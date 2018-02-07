package in.oogway.plumbox.launcher.library.config;

import redis.clients.jedis.Jedis;

public class Config {

    // Declaring Jedis Object here. Can be changed while integration of code.
    public static Jedis jedis;

    public static String getDirPath(String key) {
        return System.getProperty(key);
    }
}