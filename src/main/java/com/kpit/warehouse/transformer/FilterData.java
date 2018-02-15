package com.kpit.warehouse.transformer;

/*
*   @author talina06 on 2/12/18
*/

import com.kpit.warehouse.model.Geofence;
import in.oogway.plumbox.launcher.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import scala.Serializable;

public class FilterData implements Transformer, Serializable {

    private Geofence geofence;
    SQLContext sqlc = new SQLContext(Config.sparkSession.getSession());

    /**
     * @param allEvents
     * @return Filtered Dataset with isWithinGeofence set as true or false.
     */
    // Filter the Event based on Geo fence parameters.
    /* geo fence lat, lon, radius, arraylist of all events.
    ** for a bus entity.*/
    @Override
    public Dataset<Row> run(Dataset<Row> allEvents) {
        geofence = new Geofence(18.5318058,73.85004320000007,(double)500);


        UDF2 filter = new UDF2<Double, Double, Boolean>() {
            public Boolean call(Double latitude, Double longitude) throws Exception {
                return checkInside(geofence.getLatitude(), geofence.getLongitude(), geofence.getRadius(), latitude, longitude);
            }
        };

        sqlc.udf().register("filter_data", filter, DataTypes.BooleanType);

        Dataset<Row> transformedRawDS = allEvents.withColumn("is_within_geofence",
                functions.callUDF("filter_data",
                        functions.col("latitude"), functions.col("longitude")));

        transformedRawDS.show();
        return transformedRawDS;
    }

    /** HAVERSINE FORMULA
     * @param longitude1
     * @param latitude1
     * @param longitude2
     * @param latitude2
     * @return distance in meters.
     */
    private static double calculateDistance(
            double longitude1, double latitude1,
            double longitude2, double latitude2) {
        // Haversine formula: determines great-circle distance between 2 points on the sphere.
        double c =
                Math.sin(Math.toRadians(latitude1)) *
                        Math.sin(Math.toRadians(latitude2)) +
                        Math.cos(Math.toRadians(latitude1)) *
                                Math.cos(Math.toRadians(latitude2)) *
                                Math.cos(Math.toRadians(longitude2) -
                                        Math.toRadians(longitude1));
        c = c > 0 ? Math.min(1, c) : Math.max(-1, c);
        return 3959 * 1.609 * 1000 * Math.acos(c);
    }

    /**
     *
     * @param x latitude of point
     * @param y longitude of point
     * @return true if point is inside geofence else, false
     */
    private static boolean checkInside(Double latitude, Double longitude, Double radius, double x, double y) {
        return calculateDistance(
                latitude, longitude, x, y
        ) < radius;
    }
}