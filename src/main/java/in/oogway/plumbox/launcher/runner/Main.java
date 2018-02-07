package in.oogway.plumbox.launcher.runner;


import in.oogway.plumbox.launcher.library.ingestor.Ingestor;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws ClassNotFoundException, IOException,
            InstantiationException, IllegalAccessException {
        String ingestorFileName = "i01";
        System.setProperty("redis_server_address", "localhost");
        Ingestor ingestorObj = new Ingestor(ingestorFileName);
        ingestorObj.execute();
    }
}