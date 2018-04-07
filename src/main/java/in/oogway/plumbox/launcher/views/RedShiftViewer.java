package in.oogway.plumbox.launcher.views;

import in.oogway.plumbox.cache.TableTopSlicer;
import in.oogway.plumbox.config.JDBCConfig;
import in.oogway.plumbox.datalake.dataLakeProperties;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by jaideep Khandelwal<jaideep@oogway.in> on 05/04/18.
 */
public interface RedShiftViewer extends Viewer, TableTopSlicer {

    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    /**
     * @return Data Type of the column for partitioning
     */
    String partitionColumnDataType();

    /*
    * HashMap to convert HiveDataType to RedShift Data Type
     */
    HashMap<String, String>  dataTypeMapping = new HashMap<String, String>() {{
        put("STRING", "VARCHAR");
        put("LONG", "BIGINT");
        put("INTEGER", "INTEGER");
        put("DECIMAL","DECIMAL(38,2)");
        put("BOOLEAN", "BOOLEAN");
        put("TIMESTAMP", "TIMESTAMP");
    }};

    /**
     * @return S3 Path
     */
    default dataLakeProperties dataLakeProperties() {
        return new dataLakeProperties(
                System.getProperty("storage_path"),
                System.getProperty("top_dir"),
                System.getProperty("storage_protocol")
        );
    }

    /**
     * @param fields dataframe fields to get the schema
     * @return (columnname datatype, columnname datatype, ....)
     */
    default String generateSQLSchema(StructField[] fields) {
        return Arrays.stream(fields).map(
                structField -> String.format("%s %s", structField.name(),
                        hiveToPgDataType(structField.dataType().typeName()))
        ).collect(Collectors.joining(", "));
    }

    /**
     * @param hiveDataType function to convert hive table to postgres datatype
     * @return redShift compatible datatype
     */
    default String hiveToPgDataType(String hiveDataType) {
        String s = hiveDataType.toUpperCase();

        if (s.startsWith("DECIMAL")) {
            s = "DECIMAL";
        }
        return dataTypeMapping.get(s);
    }

    /** Generate a table which takes fields to be used for creating a view,
     * this is independent of the dataframe.
     * @param ss Spark Session
     * @param fields Struct fields that has schema of the view to be created
     */
    @Override
    default void generateView(SparkSession ss, StructField[] fields) {

        String path = dataLakeProperties().MakePath(destinationView());

        if (checkIfTableExists()) {
            return;
        }

        String query = String.format("CREATE EXTERNAL TABLE %s.%s (%s) " +
                        "PARTITIONED BY (%s %s) " +
                        "ROW FORMAT DELIMITED " +
                        "FIELDS TERMINATED BY '\\t' "+
                        "STORED AS %s " +
                        "LOCATION '%s'", getSchemaName(), destinationView(), generateSQLSchema(fields),
                getPartitionColumn(), partitionColumnDataType(), storageFormat(), path
        );

        try {
            PreparedStatement statement = jdbcConn(query);
            statement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /** Check ff the given external table already exists for a given schema.
     * @return
     */
    default Boolean checkIfTableExists() {

        String query = String.format("SELECT TRUE WHERE EXISTS (" +
                "SELECT * FROM SVV_EXTERNAL_TABLES WHERE TABLENAME = '%s' AND SCHEMANAME='%s')",
                destinationView(),
                getSchemaName()
        );

        try {
            PreparedStatement statement = jdbcConn(query);
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                return true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return false;
    }

    /**
     * Creates a JDBC connection and executes the query
     * @throws SQLException
     */
    default PreparedStatement jdbcConn(String query) throws SQLException {

        JDBCConfig writeConfig = getWriteConfig();

        Properties props = new Properties();

        //Uncomment the following line if using a keystore.
        props.setProperty("ssl", "true");
        props.setProperty("user",  writeConfig.get().get("user"));
        props.setProperty("password", writeConfig.get().get("password"));

        Connection connection = DriverManager.getConnection(writeConfig.get().get("url"), props);

        return connection.prepareStatement(query);

    }

    /**
     * @return JDBC Write configuration
     */
    default JDBCConfig getWriteConfig() {

        JDBCConfig jdbcConfig = new JDBCConfig(String.format("%s.%s", getSchemaName(), destinationView()));
        jdbcConfig.set(new HashMap<String, String>() {{
            put("dbtable", String.format("%s.%s", getSchemaName(), destinationView()));
            put("url", System.getProperty("rs_url"));
            put("driver", "com.amazon.redshift.jdbc.Driver");
            put("user",System.getProperty("rs_user"));
            put("password",System.getProperty("rs_password"));
        }});

        return jdbcConfig;
    }

    /** Add one partition to a view
     * @param partition name of the partition to be added
     * @param viewName table name
     */
    @Override
    default void addPartition(String partition, String viewName) {

        String path = dataLakeProperties().MakePath(String.format("%s/%s", viewName, partition));

        partition = partition.replaceFirst("(?<==)(.+)", "'$1'");
        String query = String.format(
                "ALTER TABLE %s.%s ADD IF NOT EXISTS PARTITION(%s) LOCATION '%s'",
                getSchemaName(),
                viewName,
                partition,
                path
        );

        try {
            PreparedStatement statement = jdbcConn(query);
            statement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    /** Remove one particular partition from a view
     * @param partition name of the partition to be removed
     * @param viewName table name
     */
    @Override
    default void dropPartition(String partition, String viewName) {

        partition = partition.replaceFirst("(?<==)(.+)", "'$1'");

        String query = String.format(
                "ALTER TABLE %s.%s DROP PARTITION(%s)",
                getSchemaName(),
                viewName,
                partition
        );

        try {
            PreparedStatement statement = jdbcConn(query);
            statement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /** All the partitions that should be added to a view
     * @param span Number of partitions to be added to view
     * @return Set of proposed partitions
     */
    @Override
    default Set<String> proposedPartitions(Integer span) {
        /*Get a set of last 30 days from the current date
         * */
        Calendar cal = Calendar.getInstance();

        return getBackDays(span, cal, new HashSet<String>());
    }

    /** Get current partitions for the view
     * @param viewName Name of the view
     * @return Active partitions to be added.
     */
    @Override
    default Set<String> activePartitions(String viewName) {
        String query = String.format("SELECT values FROM svv_external_partitions WHERE schemaname='%s' AND tablename='%s'",
                getSchemaName(),
                destinationView()
        );

        HashSet<String> aP = new HashSet<>();

        try {
            PreparedStatement statement = jdbcConn(query);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                String value = resultSet.getString("values");

                String partition = String.format(
                        "%s=%s", getPartitionColumn(), value.replaceFirst("\\[\"(.*?)\"\\]", "$1")
                );

                aP.add(partition);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return aP;
    }

    /** Get the consecutive back days for a given span
     * @param span number of days to check value
     * @param cal Calendar instance to be used for getting dates as string in recursive function
     * @param set Append dates to the set.
     * @return
     */
    default Set<String> getBackDays(int span, Calendar cal, Set<String> set) {
        if (span > 0) {

            set.add(String.format("%s=%s", getPartitionColumn(), dateFormat.format(cal.getTime())));

            cal.add(Calendar.DATE, -1);

            return getBackDays(span - 1, cal, set);

        } else {
            return set;
        }
    }
}