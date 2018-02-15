package com.kpit.warehouse.transformer;

import com.kpit.warehouse.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import scala.Serializable;

public class ConfidenceScoring implements Transformer, Serializable {


    /**
     * Calculates the confidence probability.
     * Calculate the confidence per bus. (Number of times the bus is outside the geo fence per day)
     * (filteredEvents/totalEvents) for one bus.
     * @return confidence score/day.
     */
    @Override
    public Dataset<Row> run(Dataset<Row> events) {

        // score for that day.
        double totalEvents = 0;
        double filteredEvents = 0;
        filteredEvents = events.filter(functions.col("is_within_geofence").like("true")).count();
        totalEvents = events.count();
        Double score = (Math.round((filteredEvents/totalEvents) * 100D) / 100D);

        // Appends the score to Redis list.
        Config.jedisHelper.addToRedisList(Config.redisListName, events.head().get(1).toString(), score.toString(), Config.WINDOW);

        return events;
    }


}
