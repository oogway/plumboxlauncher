package com.kpit.warehouse.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

@JsonIgnoreProperties
public class Event implements Serializable {

    // Reading from JSON input file.
    @JsonProperty(value = "latitude")
        private Double latitude;

    @JsonProperty(value = "longitude")
    private  Double longitude;

    @JsonProperty(value = "timestamp")
    private  String timestamp;

    @JsonProperty(value = "busID")
    private  String busID;

    @JsonProperty(value = "boxID")
    private  String boxID;
}