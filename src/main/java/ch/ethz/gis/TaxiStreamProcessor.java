package ch.ethz.gis;

import ch.ethz.gis.pipelines.CartesianPipeline;
import ch.ethz.gis.pipelines.TaxiStreamPipeline;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Geometry;

/**
 * Main class that runs the processing pipeline.
 */
public class TaxiStreamProcessor {
    public static void main(String[] args) throws InterruptedException {
        if (false) {
            // Add this to make Hadoop work (this needs to be adjusted on different PCs).
            System.setProperty("hadoop.home.dir", "C:\\Programs\\Hadoop-adds");

            // Log only ERROR messages.
            StreamingContext.getInstance().sparkContext().setLogLevel("ERROR");

            // Build our pipeline.
            TaxiStreamPipeline pipeline = new CartesianPipeline();
            pipeline.buildPipeline();

            // Start the streaming pipeline.
            StreamingContext.getInstance().start();
            StreamingContext.getInstance().awaitTermination();
            StreamingContext.getInstance().stop(true, true);
        }

        double lon = 8.5;
        double lat = 47.5;
        RTree<String, Geometry> tree = RTree.create();
        tree = tree.add("id", Geometries.pointGeographic(lon, lat));
        tree = tree.add("id2", Geometries.pointGeographic(lon+0.01, lat));
        tree = tree.add("id3", Geometries.pointGeographic(lon, lat+0.01));

        tree.search(Geometries.pointGeographic(8.5, 47.5), 5.0).forEach(System.out::println);
        tree.nearest(Geometries.pointGeographic(8.5, 47.5), 5.0, 2).forEach(System.out::println);
        System.out.println();

        tree = tree.delete("id", Geometries.pointGeographic(lon, lat));

        tree.search(Geometries.pointGeographic(8.5, 47.5), 5.0).forEach(System.out::println);
        tree.nearest(Geometries.pointGeographic(8.5, 47.5), 5.0, 2).forEach(System.out::println);
    }
}
