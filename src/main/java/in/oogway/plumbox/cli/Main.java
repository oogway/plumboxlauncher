package in.oogway.plumbox.cli;

import org.apache.commons.cli.ParseException;

public class Main {

    public static void main (String args[]) throws IllegalAccessException, ClassNotFoundException, InstantiationException, ParseException {

        Cli executor = new Cli();
        executor.execute(args);

    }
}