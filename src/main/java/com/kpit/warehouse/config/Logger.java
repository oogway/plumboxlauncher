package com.kpit.warehouse.config;

import org.apache.log4j.varia.NullAppender;

public class Logger {

    public static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(Logger.class);

    public Logger(){
        org.apache.log4j.BasicConfigurator.configure(new NullAppender());
    }
}
