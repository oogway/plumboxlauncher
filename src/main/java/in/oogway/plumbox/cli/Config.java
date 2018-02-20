package in.oogway.plumbox.cli;

/*
 *   @author talina06 on 2/12/18
 */public class Config {
    public static String MASTER="local[*]";
    public static SparkConfig sparkSession;

    public static String getDirPath(String key) {
        return System.getProperty(key);
    }
}