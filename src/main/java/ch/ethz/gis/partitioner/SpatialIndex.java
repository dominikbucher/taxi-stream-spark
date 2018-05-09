package ch.ethz.gis.partitioner;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Geometry;

import java.io.Serializable;
import java.util.List;

public class SpatialIndex implements Serializable {
    private RTree<Integer, Geometry> rTree;
    private double lon;
    private double lat;

    public SpatialIndex(double lon, double lat) {
        this.lon = lon;
        this.lat = lat;

        this.rTree = RTree.create();
    }

    public RTree<Integer, Geometry> getrTree() {
        return rTree;
    }

    public Point getPoint() {
        return new Point(lon, lat);
    }

    public void add(int id, Point p) {
        rTree = rTree.add(id, Geometries.pointGeographic(p.getLon(), p.getLat()));
    }

    public List<Entry<Integer, Geometry>> search(Point p, double maxDistance) {
        return rTree.search(p.toRTreePoint(), maxDistance).toList().toSingle().toBlocking().value();
    }

    public List<Entry<Integer, Geometry>> nearest(Point p, double maxDistance, int maxCount) {
        return rTree.nearest(p.toRTreePoint(), maxDistance, maxCount).toList().toSingle().toBlocking().value();
    }
}
