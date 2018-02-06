package in.oogway.library.storage;

import in.oogway.library.config.Config;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public interface LocalStorage extends StorageDriver {

    // todo private String dirPath = Config.tempPath;

    /**
     * @param path A file path
     * @return byte array
     * @throws IOException
     */
    @Override
    public default byte[] read(String path) {

        File file = new File(path);
        byte[] bytes = new byte[0];
        try {
            InputStream is = new FileInputStream(file);
            bytes = IOUtils.toByteArray(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bytes;
    }


    @Override
    public default  void write(String path, byte[] byteArray) {

    }

}
