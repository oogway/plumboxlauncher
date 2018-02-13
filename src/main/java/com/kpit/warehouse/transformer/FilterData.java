package com.kpit.warehouse.transformer;

/*
*   @author talina06 on 2/12/18
*/

import com.kpit.warehouse.model.Geofence;
import in.oogway.plumbox.launcher.config.Config;
import org.apache.spark.sql.*;

public class FilterData implements Transformer {

    private Geofence geofence;

    @Override
    public Dataset<Row> run(Dataset<Row> allEvents) {
        geofence = new Geofence(18.5318058,73.85004320000007,(double)500);
        return filterData(geofence, allEvents);
    }

    /**
     * @param geofence
     * @param allEvents
     * @return Filtered Dataset with isWithinGeofence set as true or false.
     */
    // Filter the Event based on Geo fence parameters.
    /* geo fence lat, lon, radius, arraylist of all events.
    ** for a bus entity.*/
    public static Dataset<Row> filterData(Geofence geofence, Dataset<Row> allEvents){
        Dataset<Row> filteredSet = allEvents.withColumn( "isWithinGeofence", functions.lit(false));

        SQLContext sqlc = new SQLContext(Config.sparkSession.getSession());

        final Dataset<Row> finalSet = sqlc.createDataFrame(filteredSet.javaRDD().map(row -> {
            return RowFactory.create(row.get(0), row.get(1), row.get(2), row.get(3), row.get(4), checkInside(geofence.getLatitude(),
                    geofence.getLongitude(), geofence.getRadius(), row.getDouble(2), row.getDouble(3)));
        }), filteredSet.schema());

        return finalSet;
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