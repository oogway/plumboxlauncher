package in.oogway.library.storage;

import java.io.IOException;

public interface StorageDriver {
    byte[] read(String path) throws IOException;
    void write(String path, byte[] byteArray);
}