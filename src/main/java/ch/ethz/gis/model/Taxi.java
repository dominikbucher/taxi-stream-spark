package ch.ethz.gis.model;

import java.io.Serializable;

public class Taxi implements Serializable {
    private int id;
    private double lon;
    private double lat;
    private int numPassengers;

    private double destLon;
    private double destLat;
    private double resLon;
    private double resLat;

    public Taxi(int id) {
        this.id = id;
    }

    // AUTOGEN ================================================================
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public int getNumPassengers() {
        return numPassengers;
    }

    public void setNumPassengers(int numPassengers) {
        this.numPassengers = numPassengers;
    }

    public double getDestLon() {
        return destLon;
    }

    public void setDestLon(double destLon) {
        this.destLon = destLon;
    }

    public double getDestLat() {
        return destLat;
    }

    public void setDestLat(double destLat) {
        this.destLat = destLat;
    }

    public double getResLon() {
        return resLon;
    }

    public void setResLon(double resLon) {
        this.resLon = resLon;
    }

    public double getResLat() {
        return resLat;
    }

    public void setResLat(double resLat) {
        this.resLat = resLat;
    }
}
