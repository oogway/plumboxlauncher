package in.oogway.plumbox.launcher;

import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * Created by jaideep Khandelwal<jaideep@oogway.in> on 23/02/18.
 */
public class HadoopSource {
    //Hadoop Options to be set
    HashMap<String, String> hadoopOptions = new HashMap<>();

    //Options should have these keys to be set
    ArrayList<String> requiredKeys = new ArrayList<String>(
            Arrays.asList("fs.s3n.awsAccessKeyId", "fs.s3n.awsSecretAccessKey")
    );

    //Set command line options
    public HadoopSource(HashMap<String, String> hOpts) {
       hadoopOptions = hOpts;
    }

    /** Set all the hadoopConfiguration keys
     * @param ss Spark Session
     */
    public void load(SparkSession ss) {
        for (String key: hadoopOptions.keySet()) {
            if (requiredKeys.contains(key)) {
                ss.sparkContext().hadoopConfiguration().set(key, hadoopOptions.get(key));
            }
        }
    }
}
