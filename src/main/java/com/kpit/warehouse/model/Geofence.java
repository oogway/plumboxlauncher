package com.kpit.warehouse.model;

import java.io.Serializable;

// Immutable class
public final class Geofence implements Serializable {

    private final double latitude;
    private final double longitude;
    private final double radius;

    public Geofence( double latitude, double longitude, double radius){
        this.latitude = latitude;
        this.longitude = longitude;
        this.radius = radius;
    }

    public double getLongitude() {
        return longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getRadius() {
        return radius;
    }
}
