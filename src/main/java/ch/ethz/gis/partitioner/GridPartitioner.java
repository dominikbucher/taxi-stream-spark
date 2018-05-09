package ch.ethz.gis.partitioner;

import ch.ethz.gis.model.Taxi;
import org.apache.commons.lang.math.IntRange;
import org.apache.spark.Partitioner;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class GridPartitioner extends Partitioner implements Serializable {

    private double left;
    private double top;
    private double right;
    private double bottom;
    private int cellsX;
    private int cellsY;
    private double cellsXSize;
    private double cellsYSize;

    private int numPartitions;

    public GridPartitioner(double left, double top, double right, double bottom,
                           int cellsX, int cellsY) {
        this.left = left;
        this.top = top;
        this.right = right;
        this.bottom = bottom;
        this.cellsX = cellsX;
        this.cellsY = cellsY;
        this.cellsXSize = (right - left) / cellsX;
        this.cellsYSize = (top - bottom) / cellsY;

        this.numPartitions = cellsX * cellsY;
    }

    @Override
    public int numPartitions() {
        // We make one overflow partition for things that are out of bounds.
        return numPartitions + 1;
    }

    @Override
    public int getPartition(Object key) {
        if (key instanceof Point) {
            Point p = (Point) key;
            double offsetX = p.getLon() - this.left;
            double offsetY = p.getLat() - this.bottom;

            int x = (int) (offsetX / cellsXSize);
            int y = (int) (offsetY / cellsYSize);
            int partition = x + y * cellsX;

            // If something is out of bounds, we place it in the overflow partition.
            if (partition >= numPartitions || partition < 0) {
                return numPartitions;
            } else {
                return partition;
            }
        } else {
            // Other things are always placed in the overflow partition.
            return numPartitions;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GridPartitioner that = (GridPartitioner) o;
        return Double.compare(that.left, left) == 0 &&
                Double.compare(that.top, top) == 0 &&
                Double.compare(that.right, right) == 0 &&
                Double.compare(that.bottom, bottom) == 0 &&
                cellsX == that.cellsX &&
                cellsY == that.cellsY;
    }

    public List<SpatialIndex<Taxi>> createIndexes() {
        List<SpatialIndex<Taxi>> indexes = new ArrayList<>();
        for (int i : new IntRange(0, cellsX - 1).toArray()) {
            for (int j : new IntRange(0, cellsY - 1).toArray()) {
                SpatialIndex<Taxi> si = new SpatialIndex<>(left + ((double) i + 0.5) * cellsXSize, bottom + ((double) j + 0.5) * cellsYSize);
                System.out.println("Created Spatial Index at (" + si.getPoint().getLon() + ", " + si.getPoint().getLat() + ")");
                indexes.add(si);
            }
        }

        return indexes;
    }
}
