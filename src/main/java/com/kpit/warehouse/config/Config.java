package com.kpit.warehouse.config;

/*
*   @author talina06 on 2/13/18
*/

public class Config {
    public static final double THRESHOLD = 0.3;
    public static final int WINDOW = 5;
    public static final String redisListName = "box_confidence_scores";
    public static final String redisServerAddress = "localhost";
    public static final JedisHelper jedisHelper = new JedisHelper(redisServerAddress);

}
