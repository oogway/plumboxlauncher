package in.oogway.plumbox.launcher.library.storage;

/*
*   @author talina06 on 2/6/18
*/
public interface StorageDriver {
    byte[] read(String path);
    void write(String path, byte[] byteArray);
}