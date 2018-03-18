package in.oogway.plumbox.launcher;

import in.oogway.plumbox.launcher.storage.FileStorage;
import in.oogway.plumbox.launcher.storage.LauncherStorage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;

class LauncherStorageFileTest<T> {

    private static final String dir = "/tmp/plumbox-file";
    private FileStorage driver;
    private LauncherStorage<T> lStore;

    @BeforeEach
    void setUp() throws IOException {
        driver = new FileStorage(dir);
        driver.deleteRecursive();
        lStore = new LauncherStorage<>(driver);
    }

    @Test
    void writeAndRead() {
        String key = lStore.write("hello", "test");

        HashMap<String, T> out = lStore.readAll("test", (Class<T>) String.class);
        assert out.size() == 1;
        assert out.containsKey(key);
    }
}