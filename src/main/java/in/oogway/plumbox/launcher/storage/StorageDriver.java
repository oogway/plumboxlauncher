package in.oogway.plumbox.launcher.storage;

import java.io.UnsupportedEncodingException;
import java.util.Map;

/*
*   @author talina06 on 2/6/18
*/
public interface StorageDriver {
    byte[] read(String path);
}
