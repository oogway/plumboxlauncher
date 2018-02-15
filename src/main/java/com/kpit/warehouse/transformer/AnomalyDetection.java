package com.kpit.warehouse.transformer;

import com.kpit.warehouse.config.Config;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import scala.Serializable;

import java.sql.Timestamp;
import java.util.List;

import static in.oogway.plumbox.launcher.config.Config.sparkSession;

public class AnomalyDetection implements Transformer, Serializable {


    @Override
    public Dataset<Row> run(Dataset<Row> events) {

        // Latest date's score is saved in lowest index(0).
        List<String> strScores = Config.jedisHelper.getList(Config.redisListName, events.head().get(1).toString());

        // for each event which has column as true. do ++
        UDF2 anomaly_registering = new UDF2<String, String, List<String>>() {
            public List<String> call(String depotID, String boxID) throws Exception {

                double median = calculateMedian(strScores);
                if(median > Config.THRESHOLD){
                    return strScores;
                }
                return null;
            }
        };

        SQLContext sqlc = new SQLContext(sparkSession.getSession());
        sqlc.udf().register("anomaly_registering", anomaly_registering, DataTypes.BooleanType);

        Dataset<Row> transformedRawDS = events.withColumn("scores",
                functions.callUDF("anomaly_registering",
                        functions.col("depotID"), functions.col("boxID")));

        transformedRawDS = transformedRawDS.select(functions.col("boxID"), functions.col("depotID"))
                .withColumn("savedOn", functions.current_timestamp())
                .where(transformedRawDS.col("scores").isNotNull());


        return transformedRawDS;

    }

    /**
     * Saves all anomalies (Box which is outside the geofence too often) as a separate object entry.
     */
    private double calculateMedian(List<String> scores) {
        // Get a DescriptiveStatistics instance
        DescriptiveStatistics stats = new DescriptiveStatistics();
        // Add the data from the array
        for (int i = 0; i < scores.size(); i++) {
            stats.addValue(Double.parseDouble(scores.get(i)));
        }
        return stats.getPercentile(50);
    }
}