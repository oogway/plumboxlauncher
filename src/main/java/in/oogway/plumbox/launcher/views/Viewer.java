package in.oogway.plumbox.launcher.views;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

/**
 * Created by jaideep Khandelwal<jaideep@oogway.in> on 06/04/18.
 */
public interface Viewer {

    /**
     * @return Column name on which partition has to be done
     */
    String getPartitionColumn();

    /**
     * @return Format of the storage
     */
    String storageFormat();

    /**
     * @return Name of the schema
     */
    String getSchemaName();



    void generateView(SparkSession ss, StructField[] fields);
}
