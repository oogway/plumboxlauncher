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

<<<<<<< HEAD
    private String[] new_transformations() {
        return new String[]{
                "in.oogway.plumbox.ActionsTransformation",
                "in.oogway.plumbox.ShowDf"
        };
    }

=======
>>>>>>> 97bb46525d9bcf1d00d4279245b3ba6b15f1ca35
    @Test
    void testIngester() {
        Plumbox pb = new Plumbox(new MemoryStorage());

        Source s = new Source(new HashMap<String, String>(){{
<<<<<<< HEAD
//          put("path", "src/test/resources/input_source_file.json");
//          put("format", "json");
=======
//            put("path", "src/test/resources/input_source_file.json");
//            put("format", "json");
>>>>>>> 97bb46525d9bcf1d00d4279245b3ba6b15f1ca35
            put("format", "jdbc");
            put("driver", "com.mysql.jdbc.Driver");
            put("url", "jdbc:mysql://127.0.0.1:3306/test?user=reader&password=10.0.0.1");
            put("dbtable", "(SELECT * FROM users WHERE phone=1) AS x");
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
<<<<<<< HEAD

    @Test
    void testMongoIngester() {
        Plumbox pb = new Plumbox(new MemoryStorage());

        Source s = new Source(new HashMap<String, String>(){{
            put("format", "com.mongodb.spark.sql.DefaultSource");
            put("driver", "org.mongodb.spark:mongo-spark-connector_2.11:2.2.0");
            put("pipeline", "[{$match:{organization:ObjectId('5719c66d1a6cca0a00c1199e')}},{$project:{user_created:1,team:1,date_created:1,state:1}}]");
            put("uri", "mongodb://oogway:ZTilKoeqioTDIuUI@sc-prod-clone-shard-00-00-ytkwl.mongodb.net:27017,sc-prod-clone-shard-00-01-ytkwl.mongodb.net:27017,sc-prod-clone-shard-00-02-ytkwl.mongodb.net:27017/safetyapps_production.actionapp.actions?ssl=true&replicaSet=sc-prod-clone-shard-0&authSource=admin");
            //put("dbtable", "(SELECT * FROM users WHERE phone=1) AS x");
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
=======
>>>>>>> 97bb46525d9bcf1d00d4279245b3ba6b15f1ca35
}

