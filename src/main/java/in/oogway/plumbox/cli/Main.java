package in.oogway.plumbox.cli;

import in.oogway.plumbox.launcher.storage.FileStorage;
import in.oogway.plumbox.launcher.storage.LauncherStorageDriver;
import in.oogway.plumbox.launcher.storage.RedisStorage;

public class Main {
    public static void main(String args[]) throws Exception {
        Cli executor = new Cli();

        LauncherStorageDriver driver;

        String storagePath = System.getProperty("storage_path", "");
        if (storagePath != "") {
            driver = new FileStorage(storagePath);
        } else {
            String redisHost = System.getProperty("REDIS_HOST", "localhost");
            driver = new RedisStorage(redisHost);
        }

        executor.execute(args, driver);
    }
}