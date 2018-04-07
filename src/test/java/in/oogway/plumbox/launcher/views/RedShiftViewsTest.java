package in.oogway.plumbox.launcher.views;

import in.oogway.plumbox.cli.testing.LocalTester;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Created by jaideep Khandelwal<jaideep@oogway.in> on 06/04/18.
 */
class RedShiftViewsTest extends LocalTester {

    @Before
    void setUp() {

        localSession()
                .sparkContext()
                .hadoopConfiguration()
                .set("fs.s3n.awsAccessKeyId","");

        localSession()
                .sparkContext()
                .hadoopConfiguration()
                .set("fs.s3n.awsSecretAccessKey","");

        System.setProperty("rs_url", "");
        System.setProperty("rs_user", "");
        System.setProperty("rs_password", "");
        System.setProperty("storage_path", "");
        System.setProperty("storage_protocol", "");
        System.setProperty("top_dir", "");

    }

    @Test
    void testViewCreation() {
        setUp();
        RedShiftViews redShiftViews = new RedShiftViews();

        StructType schema = ExpressionEncoder.javaBean(ViewBean.class).schema();

        redShiftViews.generateView(localSession(), schema.fields());

        redShiftViews.rePopulate();
    }
}