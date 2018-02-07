package in.oogway.plumbox.launcher.library.storage;

import in.oogway.plumbox.launcher.library.config.Config;
import org.apache.commons.io.IOUtils;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.nio.charset.Charset;
import java.util.List;

public interface RedisStorage  extends StorageDriver {

    void redisServer() throws IOException;

    /**
     * @param key A key whose value is to be read. Eg: source_id: <source yaml file as value>
     * @return byte array of the yaml file contents.
     * @throws IOException
     */
    @Override
    default byte[] read(String key) throws IOException {
        Config.jedis.ping();
        String contents = Config.jedis.get(key);
        return contents.getBytes(Charset.forName("UTF-8"));
    }

    /**
     * @param key
     * @param byteArray
     * @throws IOException
     */
    @Override
    default  void write(String key, byte[] byteArray) throws IOException {
        // write to redis.
        Config.jedis.set(key, new String(byteArray));
    }
}
