package in.oogway.plumbox.cli;

import in.oogway.plumbox.launcher.*;
import in.oogway.plumbox.launcher.storage.FileStorage;
import in.oogway.plumbox.launcher.storage.LauncherStorage;
import in.oogway.plumbox.launcher.storage.RedisStorage;
import in.oogway.plumbox.launcher.storage.LauncherStorageDriver;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class Launcher {
    public static void main(String args[]) throws IllegalAccessException, ClassNotFoundException, InstantiationException, IOException {

        SparkSession ss = SparkSession
                .builder()
                .appName("Plumbox Launcher")
                .getOrCreate();

        LauncherStorageDriver driver;
        if (System.getProperty("storage_path") != "") {
            driver = new FileStorage(System.getProperty("storage_path"));
        } else {
            driver = new RedisStorage(System.getProperty("redis_host"));
        }

        // Read from system properties
        String ingesterId = System.getProperty("ingester_id");

        LauncherStorage<Ingester> storage = new LauncherStorage<>(driver);
        Ingester i = storage.read(ingesterId, Ingester.class);
        i.execute(storage, ss);
    }
}
