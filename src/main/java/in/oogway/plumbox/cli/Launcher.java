package in.oogway.plumbox.cli;

import in.oogway.plumbox.launcher.*;
import in.oogway.plumbox.launcher.storage.FileStorage;
import in.oogway.plumbox.launcher.storage.LauncherStorage;
import in.oogway.plumbox.launcher.storage.RedisStorage;
import in.oogway.plumbox.launcher.storage.LauncherStorageDriver;
import org.apache.spark.sql.SparkSession;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;

public class Launcher {
    private static HashMap<String, Object> makeOptions(String opt) throws IOException {
        HashMap<String, Object> opts = new HashMap<>();
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(opt, opts.getClass());
    }

    public static void main(String args[]) throws IllegalAccessException, ClassNotFoundException, InstantiationException, IOException {
        if (args.length == 0) {
            throw new IllegalArgumentException("Need valid ingester_id as first argument");
        }

        if (args.length > 2) {
            throw new IllegalArgumentException("Greedy for arguments? Launcher only supports two. ingester_id followed by JSON string");
        }

        SparkSession ss = SparkSession
                .builder()
                .appName("Plumbox Launcher")
                .getOrCreate();

        LauncherStorageDriver driver;

        String storagePath = System.getProperty("storage_path", "");
        if (!storagePath.equals("")) {
            driver = new FileStorage(storagePath);
        } else {
            String redisHost = System.getProperty("REDIS_HOST", "localhost");
            driver = new RedisStorage(redisHost);
        }

        LauncherStorage<Ingester> storage = new LauncherStorage<>(driver);
        Ingester i = storage.read(args[0], Ingester.class);
        if (args.length == 2) {
            i.execute(storage, ss, makeOptions(args[1]));
        } else {
            i.execute(storage, ss);
        }
    }
}
