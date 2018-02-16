package in.oogway.plumbox.cli;

import net.sourceforge.argparse4j.inf.Namespace;

public interface CliHandler {
    void run(Plumbox pb, Namespace ns) throws IllegalAccessException, ClassNotFoundException, InstantiationException;
}
