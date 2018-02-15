package in.oogway.plumbox.launcher.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.input.CharSequenceReader;

import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

/*
*   @author talina06 on 2/15/18
*/

public interface JSONParser extends RedisStorage{

    default Map loadContent(String id)
            throws IllegalAccessException, InstantiationException, ClassNotFoundException, IOException {
        byte[] bytes = read(id);
        Reader fileReader = getFileReader(bytes);
        return getJSONMap(fileReader);
    }

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
}
