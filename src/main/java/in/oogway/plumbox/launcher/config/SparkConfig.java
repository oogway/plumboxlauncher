package in.oogway.plumbox.launcher.config;

import org.apache.spark.sql.SparkSession;

/*Create Spark session*/
public class SparkConfig {

    private SparkSession spark;

    public SparkConfig(String appname) {
        this.spark = SparkSession
                .builder()
                .master(Config.MASTER)
                .appName(appname)
                .getOrCreate();
    }

    public SparkSession getSession() {
        return this.spark;
    }
}

