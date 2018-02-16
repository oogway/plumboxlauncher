package in.oogway.plumbox.cli;

import in.oogway.plumbox.launcher.Ingester;
import in.oogway.plumbox.launcher.Sink;
import in.oogway.plumbox.launcher.Source;
import in.oogway.plumbox.launcher.Transformation;
import org.junit.jupiter.api.Test;

class PlumboxTest extends LocalTester {
    private String[] transformations() {
        return new String[]{
                "in.oogway.plumbox.SampleTransformation",
                "in.oogway.plumbox.ShowDf"};
    }

    @Test
    void testIngester() {
        Plumbox pb = new Plumbox(new MemoryStorage());

        Source s = new Source("src/test/resources/input_source_file.json", "json", "local");
        String sourceId = pb.declare(s);

        Sink sk = new Sink("json", "output", "");
        String sinkId = pb.declare(sk);

        Transformation t = new Transformation("".join(",", transformations()));
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

