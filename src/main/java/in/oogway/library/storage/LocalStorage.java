package in.oogway.library.storage;

import in.oogway.library.config.Config;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public interface LocalStorage extends StorageDriver {

    Config config = new Config();
    String dir = config.getDirPath("yamldirectory");
    String ext = config.getDirPath("ext");

    /**
     * @param path A file path
     * @return byte array
     * @throws IOException
     */
    @Override
    public default byte[] read(String path) throws IOException {
        File file = new File(dir + path + ext);
        InputStream is = new FileInputStream(file);
        byte[] bytes = IOUtils.toByteArray(is);
        if (is != null) {
            is.close();
        }
        return bytes;
    }

    @Override
    public default  void write(String path, byte[] byteArray) {

    }

}
