package in.oogway.plumbox.cli.testing;

import org.apache.spark.sql.SparkSession;

public class LocalTester {
    protected static SparkSession localSession() {
        SparkSession session = SparkSession
            .builder()
            .master("local")
            .appName("Plumbox Launcher Test")
            .getOrCreate();

        session.sparkContext().setLogLevel("ERROR");
        session.conf().set("spark.sql.shuffle.partitions", 2);
        return session;
    }
}
