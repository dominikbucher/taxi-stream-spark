package ch.ethz.gis.partitioner;

import com.github.davidmoten.rtree.geometry.Geometries;

import java.util.Objects;

public class Point {
    private double lon;
    private double lat;

    public Point(double lon, double lat) {
        this.lon = lon;

        this.lat = lat;
    }

    public com.github.davidmoten.rtree.geometry.Point toRTreePoint() {
        return Geometries.pointGeographic(this.getLon(), this.getLat());
    }

    public double getLat() {
        return lat;
    }

    public double getLon() {

        return lon;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Point point = (Point) o;
        return Double.compare(point.lon, lon) == 0 &&
                Double.compare(point.lat, lat) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lon, lat);
    }
}
