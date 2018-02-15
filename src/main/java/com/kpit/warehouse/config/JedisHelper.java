package com.kpit.warehouse.config;

import redis.clients.jedis.Jedis;

import java.util.List;

public class JedisHelper {

    private Jedis jedis;
    private String address;
    private Logger log = new Logger();
    public JedisHelper(String address){
        this.address = address;
        //Connecting to Redis server on localhost
        jedis = new Jedis(address);
        log.LOGGER.info("Connected to Redis server sucessfully");

        // jedis.flushDB();
        //check whether server is running or not
        log.LOGGER.info("Server is running: " + jedis.ping());
    }

    public void addToRedisList(String listName, String boxID, String score, int window){
        // check if list already has entries as per window size.
        List<String> items = getList(listName, boxID);
        // trim the list to store scores only for the latest x days. x is window.
        if(items.size() == window)
            jedis.ltrim(listName + boxID, 0, window-1);

        //store data in redis list
        jedis.lpush(listName + boxID, score);
    }

    public List<String> getList(String listName, String boxID){
            return jedis.lrange(listName + boxID, 0, -1);
    }

}