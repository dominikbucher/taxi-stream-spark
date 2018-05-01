package ch.ethz.gis.model;

import java.io.Serializable;

public class ClientRequest implements Serializable {
    private int id;
    private double lon;
    private double lat;
    private double destLon;
    private double destLat;

    public ClientRequest(int id) {
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
}
