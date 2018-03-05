package in.oogway.plumbox.cli;

import in.oogway.plumbox.cli.testing.LocalTester;
import in.oogway.plumbox.cli.testing.MemoryStorage;
import in.oogway.plumbox.launcher.Ingester;
import in.oogway.plumbox.launcher.Pipeline;
import in.oogway.plumbox.launcher.Sink;
import in.oogway.plumbox.launcher.Source;
import org.junit.jupiter.api.Test;

class PlumboxTest extends LocalTester {
    private String[] transformations() {
        return new String[]{
                // Always add a new custom source transformation in the pipeline before other transformations.
                "in.oogway.plumbox.JSONSource",
                "in.oogway.plumbox.SampleTransformation",
                "in.oogway.plumbox.cli.testing.transformation.ShowDF"
        };
    }
    @Test
    void testIngester() {
        System.setProperty("format","json");
        System.setProperty("multiline", "true");
        System.setProperty("path","src/test/resources/input_source_file.json");


        Plumbox pb = new Plumbox(new MemoryStorage());

        // Always create a dummy source.
        Source s = new Source();

        String sourceId = pb.declare(s);

        Sink sk = new Sink("json", "output", "");
        String sinkId = pb.declare(sk);

        Pipeline t = new Pipeline("".join(",", transformations()));
        String tId = pb.declare(t);

        Ingester ig = new Ingester(sourceId, sinkId, tId);
        String iId = pb.declare(ig);

        try {
            ig.execute(pb.getDriver(), localSession());
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
