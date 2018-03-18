package in.oogway.plumbox.cli;

import in.oogway.plumbox.launcher.storage.LauncherStorage;
import in.oogway.plumbox.launcher.storage.LauncherStorageDriver;

import java.util.HashMap;

public class Plumbox<T> {
    private LauncherStorage<T> driver;

    public Plumbox(LauncherStorageDriver driver) {
        this.driver = new LauncherStorage(driver);
    }

    public LauncherStorage<T> getDriver() {
        return driver;
    }

    public String declare(T inp) {
        String prefix = inp.getClass().getSimpleName().toLowerCase();
        return driver.write(inp, prefix);
    }

    public T get(String id, Class<T> cls) {
        return driver.read(id, cls);
    }

    public HashMap<String, T> getAll(String pattern, Class<T> cls) {
        return driver.readAll(pattern, cls);
    }
}