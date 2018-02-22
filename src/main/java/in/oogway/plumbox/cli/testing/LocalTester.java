package in.oogway.plumbox.cli.testing;

import org.apache.spark.sql.SparkSession;

public class LocalTester {
    protected static SparkSession localSession() {
        return SparkSession
                .builder()
                .master("local")
                .appName("Plumbox Launcher Test")
                .getOrCreate();
    }
}
