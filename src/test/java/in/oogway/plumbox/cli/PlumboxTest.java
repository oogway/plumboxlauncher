package in.oogway.plumbox.cli;

import in.oogway.plumbox.launcher.Ingester;
import in.oogway.plumbox.launcher.Pipeline;
import in.oogway.plumbox.launcher.Sink;
import in.oogway.plumbox.launcher.Source;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

class PlumboxTest extends LocalTester {
    private String[] transformations() {
        return new String[]{
                "in.oogway.plumbox.SampleTransformation",
                "in.oogway.plumbox.ShowDf"
        };
    }

    private String[] new_transformations() {
        return new String[]{
                "in.oogway.plumbox.ActionsTransformation",
                "in.oogway.plumbox.ShowDf"
        };
    }

    /*
    @Test
    void testIngester() {
        Plumbox pb = new Plumbox(new MemoryStorage());

        Source s = new Source(new HashMap<String, String>(){{
            put("format", "jdbc");
            put("driver", "com.mysql.jdbc.Driver");
            put("url", "jdbc:mysql://127.0.0.1:3306/test_db?user=root&password=password");
            put("dbtable", "student");
        }});

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
    */

    @Test
    void testMongoIngester() {
        Plumbox pb = new Plumbox(new MemoryStorage());

        String input_uri = "mongodb://oogway:10.0.0.1@changerdemo-shard-00-00-vi20w.mongodb.net:27017,changerdemo-shard-00-01-vi20w.mongodb.net:27017,changerdemo-shard-00-02-vi20w.mongodb.net:27017/test.actionapp.actions?ssl=true&replicaSet=changerdemo-shard-0&authSource=admin";

        Source s = new Source(new HashMap<String, String>(){{
            //put("format", "jdbc");
            //put("driver", "com.mysql.jdbc.Driver");
            put("uri", input_uri);
            //put("dbtable", "student");
        }});


        String sourceId = pb.declare(s);

        Sink sk = new Sink("json", "output", "");
        String sinkId = pb.declare(sk);

        Pipeline t = new Pipeline("".join(",", new_transformations()));
        String tId = pb.declare(t);

        Ingester ig = new Ingester(sourceId, sinkId, tId);
        String iId = pb.declare(ig);

        try {
            ig.execute(pb.getDriver(), localMongoSession(input_uri));

        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}

