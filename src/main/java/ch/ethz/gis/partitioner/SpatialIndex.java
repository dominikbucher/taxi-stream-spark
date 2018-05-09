package ch.ethz.gis.partitioner;

import ch.ethz.gis.model.Pointable;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Geometry;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class SpatialIndex<T extends Pointable> implements Serializable {
    private RTree<Integer, Geometry> rTree;
    private HashMap<Integer, T> elementsInTree;
    private double lon;
    private double lat;

    public SpatialIndex(double lon, double lat) {
        this.lon = lon;
        this.lat = lat;

        this.elementsInTree = new HashMap<>();
        this.rTree = RTree.create();
    }

    public RTree<Integer, Geometry> getrTree() {
        return rTree;
    }

    public Point getPoint() {
        return new Point(lon, lat);
    }

    public void add(T t) {
        Point p = t.getPoint();
        elementsInTree.put(t.getId(), t);
        rTree = rTree.add(t.getId(), Geometries.pointGeographic(p.getLon(), p.getLat()));
    }

    public void remove(T t) {
        T oldT = elementsInTree.get(t.getId());
        if (oldT != null) {
            Point p = oldT.getPoint();
            elementsInTree.remove(t.getId());
            rTree = rTree.delete(t.getId(), Geometries.pointGeographic(p.getLon(), p.getLat()));
        }
    }

    public List<T> search(Point p, double maxDistance) {
        return rTree.search(p.toRTreePoint(), maxDistance).toList()
                .map(results -> results.stream().map(entry -> elementsInTree.get(entry.value()))
                        .collect(Collectors.toList())).toSingle().toBlocking().value();
    }

    public List<T> nearest(Point p, double maxDistance, int maxCount) {
        return rTree.nearest(p.toRTreePoint(), maxDistance, maxCount).toList()
                .map(results -> results.stream().map(entry -> elementsInTree.get(entry.value()))
                        .collect(Collectors.toList())).toSingle().toBlocking().value();
    }
}
