package in.oogway.plumbox.cli.testing;

import in.oogway.plumbox.cli.Plumbox;
import in.oogway.plumbox.launcher.*;
import in.oogway.plumbox.launcher.storage.MemoryStorage;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;

public class TestIngester {
    private final Ingester ingester;
    private final Plumbox pb;

    public TestIngester(String[] transformers) {
        this.pb = new Plumbox(new MemoryStorage());

        Source source = new Source(new HashMap<>());
        String sID = pb.declare(source);

        Pipeline pipeline = new Pipeline("".join(",", transformers));
        String tId= pb.declare(pipeline);

        Sink sink = new Sink("csv","output","");
        String skID = pb.declare(sink);

        Ingester ingester = new Ingester(sID, skID, tId);
        this.ingester = ingester;
    }

    public void execute(SparkSession ss) throws IllegalAccessException, ClassNotFoundException, InstantiationException {
        this.ingester.execute(this.pb.getDriver(), ss);
    }
}

