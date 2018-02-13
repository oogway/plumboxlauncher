package in.oogway.plumbox.launcher.config;

import redis.clients.jedis.Jedis;

/*
*   @author talina06 on 2/12/18
*/public class Config {
    public static String MASTER="local[*]";
    public static Jedis jedis;
    public static SparkConfig sparkSession;

    public static String getDirPath(String key) {
        return System.getProperty(key);
    }
}
