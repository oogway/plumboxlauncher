package in.oogway.plumbox.cli;

import org.apache.spark.sql.SparkSession;

public class LocalTester {
    static SparkSession localSession() {
        return SparkSession
                .builder()
                .master("local")
                .appName("Plumbox Launcher Test")
                .getOrCreate();
    }
}
