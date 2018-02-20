package in.oogway.plumbox.cli;

import java.util.HashMap;

public interface CliHandler {
    void run(Plumbox pb, HashMap<String, String> ns) throws IllegalAccessException, ClassNotFoundException, InstantiationException;
}
