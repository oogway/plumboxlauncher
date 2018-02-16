package in.oogway.plumbox.cli;

import in.oogway.plumbox.launcher.StorageDriver;

import java.util.HashMap;

public class Plumbox<T> {
    private StorageDriver<T> driver;

    public Plumbox(StorageDriver<T> driver) {
        this.driver = driver;
    }

    public StorageDriver<T> getDriver() {
        return driver;
    }

    public String declare(T inp) {
        String prefix = inp.getClass().getSimpleName().toLowerCase();
        return driver.write("", inp, prefix);
    }

    public T get(String id, Class<T> cls) {
        return driver.read(id, cls);
    }

    public HashMap<String, T> getAll(String pattern, Class<T> cls) {
        return driver.readAll(pattern, cls);
    }
}