package in.oogway.library.config;

import java.io.*;
import java.util.Properties;

public class Config {

    static final String tempPath = "src/main/java/in/oogway/runner/tmp/";

    public Config()
    {
        System.setProperty("yamldirectory", tempPath);
        System.setProperty("ext", ".yml");
    }

    public String getDirPath(String key)
    {
        return System.getProperty(key);
    }
}
