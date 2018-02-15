package in.oogway.plumbox.launcher.storage;

/*
*   @author talina06 on 2/6/18
*/
public interface StorageDriver {
    byte[] read(String path);
}
