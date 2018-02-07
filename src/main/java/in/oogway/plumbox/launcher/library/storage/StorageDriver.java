package in.oogway.plumbox.launcher.library.storage;

import java.io.FileNotFoundException;
import java.io.IOException;

public interface StorageDriver {
    byte[] read(String path);
    void write(String path, byte[] byteArray);
}