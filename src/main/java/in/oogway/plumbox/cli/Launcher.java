package in.oogway.plumbox.cli;

import in.oogway.plumbox.launcher.Ingester;
import in.oogway.plumbox.launcher.StorageDriver;
import org.apache.spark.sql.SparkSession;

public class Launcher {

    public static void main(String args[]) throws IllegalAccessException, ClassNotFoundException, InstantiationException {

        SparkSession ss = SparkSession
                .builder()
                .appName("Plumbox Launcher Test")
                .getOrCreate();

        // Read from system properties
        String ingester_id = System.getProperty("ingester_id");
        StorageDriver memDriver = new RedisStorage(System.getProperty("redis_host"));

        Ingester i = (Ingester) memDriver.read(ingester_id, Ingester.class);
        i.execute(memDriver, ss);
    }
}
