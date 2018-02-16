package in.oogway.plumbox.cli;

import net.sourceforge.argparse4j.inf.ArgumentParserException;

public class Main {

    public static void main (String args[]) throws ArgumentParserException, IllegalAccessException, ClassNotFoundException, InstantiationException {

        Cli executor = new Cli();
        executor.execute(args);

    }
}