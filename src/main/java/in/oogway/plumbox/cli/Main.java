package in.oogway.plumbox.cli;

import net.sourceforge.argparse4j.inf.ArgumentParserException;

public class Main {

    public static void main (String args[]) {

        try {
            CommandExecutor executor = new CommandExecutor();
            executor.execute(args);

        } catch (ArgumentParserException e) {
            e.printStackTrace();
        }
    }
}