package in.oogway.runner;

import in.oogway.library.ingestor.Ingestor;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws ClassNotFoundException, IOException,
            InstantiationException, IllegalAccessException {
        String ingestorFileName = "i01";

        Ingestor ingestorObj = new Ingestor();
        ingestorObj.loadContent(ingestorFileName);
    }
}