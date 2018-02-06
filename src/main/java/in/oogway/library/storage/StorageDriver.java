package in.oogway.library.storage;

public interface StorageDriver {
    byte[] read(String path);
    void write(String path, byte[] byteArray);
}