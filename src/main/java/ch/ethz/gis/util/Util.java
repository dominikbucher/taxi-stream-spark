package ch.ethz.gis.util;

public class Util {
    public static double distance(double lon1, double lat1, double lon2, double lat2) {
        return Math.sqrt((lon2 - lon1) * (lon2 - lon1) + (lat2 - lat1) * (lat2 - lat1));
    }
}
