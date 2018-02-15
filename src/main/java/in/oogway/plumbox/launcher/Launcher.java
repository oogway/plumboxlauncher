package in.oogway.plumbox.launcher;

/*
*   @author talina06 on 2/8/18
*/

import in.oogway.plumbox.launcher.ingestor.Ingestor;

import java.io.IOException;

public class Launcher {

    /**
     * @param args Will contain the ingestor_redis_key
     *             This will execute the ingestor method for the ingestor_redis_key passed.
     *             And run transformations mentioned in the yaml.
     * @throws ClassNotFoundException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws IOException
     */

    public static void main(String[] args) throws ClassNotFoundException, InstantiationException,
            IllegalAccessException, IOException {

        Ingestor ingestorObj = new Ingestor(args[0]);
        ingestorObj.execute();
    }
}