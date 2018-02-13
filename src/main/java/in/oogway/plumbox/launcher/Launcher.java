package in.oogway.plumbox.launcher;

/*
*   @author talina06 on 2/8/18
*/

import in.oogway.plumbox.launcher.ingestor.Ingestor;
import net.sourceforge.argparse4j.inf.ArgumentParserException;

import java.io.IOException;

public class Launcher {

    // todo explain what is being done.
    public static void main(String[] args) throws ClassNotFoundException, InstantiationException,
            IllegalAccessException, IOException {

        Ingestor ingestorObj = new Ingestor(args[0]);
        ingestorObj.execute();
    }
}