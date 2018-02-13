package in.oogway.plumbox.launcher.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import in.oogway.plumbox.launcher.config.Config;
import org.apache.commons.io.input.CharSequenceReader;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

/*
*   @author talina06 on 2/7/18
*/
public interface RedisStorage  extends StorageDriver {

    default void redisServer() {
        String address = Config.getDirPath("redis_server_address");
        Config.jedis = new Jedis(address);
    }

    /**
     * @param key A key whose value is to be read. Eg: source_id: <source yaml file as value>
     * @return byte array of the yaml file contents.
     */
    @Override
    default byte[] read(String key)  {
        String contents = Config.jedis.get(key);
        return contents.getBytes(Charset.forName("UTF-8"));
    }

    // todo move to relevant package.
    default Reader getFileReader(byte[] initialArray) throws IOException {
        Reader targetReader = new CharSequenceReader(new String(initialArray));
        targetReader.close();

        return targetReader;
    }

    default Map getJSONMap(Reader fileReader) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        TypeReference<HashMap<String, Object>> typeRef
                = new TypeReference<HashMap<String, Object>>() {};
        return mapper.readValue(fileReader, typeRef);
    }

    default Map loadContent(String id)
            throws IllegalAccessException, InstantiationException, ClassNotFoundException, IOException {
        byte[] bytes = read(id);
        Reader fileReader = getFileReader(bytes);
        return getJSONMap(fileReader);
    }


}
