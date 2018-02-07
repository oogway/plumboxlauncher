package in.oogway.plumbox.launcher.library.storage;

import java.io.FileNotFoundException;
import java.io.IOException;

public interface StorageDriver {
    byte[] read(String path) throws IOException;
    void write(String path, byte[] byteArray) throws IOException;
}