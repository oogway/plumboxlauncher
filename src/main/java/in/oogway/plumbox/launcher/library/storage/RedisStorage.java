package in.oogway.plumbox.launcher.library.storage;

import in.oogway.plumbox.launcher.library.config.Config;

import java.io.IOException;
import java.nio.charset.Charset;

/*
*   @author talina06 on 2/7/18
*/
public interface RedisStorage  extends StorageDriver {

    void redisServer();

    /**
     * @param key A key whose value is to be read. Eg: source_id: <source yaml file as value>
     * @return byte array of the yaml file contents.
     */
    @Override
    default byte[] read(String key)  {
        String contents = Config.jedis.get(key);
        return contents.getBytes(Charset.forName("UTF-8"));
    }

    /**
     * @param key
     * @param byteArray
     * @throws IOException
     */
    @Override
    default  void write(String key, byte[] byteArray) {
        // write to redis.
        Config.jedis.set(key, new String(byteArray));
    }
}
