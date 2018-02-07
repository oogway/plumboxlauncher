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
    public default byte[] read(String path) {
        File file = new File(dir + path + ext);
        InputStream is = null;
        byte[] bytes = new byte[0];
        try {
            is = new FileInputStream(file);
            bytes = IOUtils.toByteArray(is);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return bytes;
    }

    @Override
    public default  void write(String path, byte[] byteArray) {

    }

}
