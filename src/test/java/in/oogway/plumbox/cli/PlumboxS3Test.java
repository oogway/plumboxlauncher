package in.oogway.plumbox.cli;

import in.oogway.plumbox.cli.testing.LocalTester;
import in.oogway.plumbox.cli.testing.MemoryStorage;
import in.oogway.plumbox.launcher.*;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

class PlumboxS3Test extends LocalTester {
    private String[] transformations() {
        return new String[]{
                "in.oogway.plumbox.SampleTransformation",
                "in.oogway.plumbox.cli.testing.transformation.ShowDF"
        };
    }

    @Test
    void testIngester() {
        Plumbox pb = new Plumbox(new MemoryStorage());

        Source s = new Source(new HashMap<String, String>(){{
            put("format", "json");
            put("path", System.getenv("SOURCE_URL"));
        }});

        String sourceId = pb.declare(s);

        Sink sk = new Sink("json", "output", "");
        String sinkId = pb.declare(sk);

        Pipeline t = new Pipeline("".join(",", transformations()));
        String tId = pb.declare(t);

        Ingester ig = new Ingester(sourceId, sinkId, tId);
        String iId = pb.declare(ig);

        SparkSession ss = localSession();

        HadoopSource hadoopSource = new HadoopSource(new HashMap<String, String>(){{
                put("fs.s3n.awsAccessKeyId", System.getenv("AWS_ACCESS_KEY"));
                put("fs.s3n.awsSecretAccessKey", System.getenv("AWS_SECRET_KEY"));
        }});

        hadoopSource.load(ss);

        try {
            ig.execute(pb.getDriver(), ss);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

}

