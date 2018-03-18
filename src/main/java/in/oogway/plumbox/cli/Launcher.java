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

        String storagePath = System.getProperty("storage_path", "");
        if (storagePath != "") {
            driver = new FileStorage(storagePath);
        } else {
            String redisHost = System.getProperty("REDIS_HOST", "localhost");
            driver = new RedisStorage(redisHost);
        }

        // Read from system properties
        String ingesterId = System.getProperty("ingester_id");
        if (ingesterId == "") {
            throw new IllegalArgumentException(String.format("Need valid ingester_id. Found %s", ingesterId));
        }

        LauncherStorage<Ingester> storage = new LauncherStorage<>(driver);
        Ingester i = storage.read(ingesterId, Ingester.class);
        i.execute(storage, ss);
    }
}
