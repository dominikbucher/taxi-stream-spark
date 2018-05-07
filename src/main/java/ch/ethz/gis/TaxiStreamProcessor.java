package ch.ethz.gis;

import ch.ethz.gis.pipelines.SpatialIndexPipeline;
import ch.ethz.gis.pipelines.TaxiStreamPipeline;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Geometry;
import org.apache.commons.lang.SerializationUtils;

import java.io.Serializable;

/**
 * Main class that runs the processing pipeline.
 */
public class TaxiStreamProcessor {
    public static void main(String[] args) throws InterruptedException {
        // Add this to make Hadoop work (this needs to be adjusted on different PCs).
        System.setProperty("hadoop.home.dir", "C:\\Programs\\Hadoop-adds");
        // we need the KryoSerializer to serialize rTrees and such.
        System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        // Log only ERROR messages.
        StreamingContext.getInstance().sparkContext().setLogLevel("ERROR");

        // Build our pipeline.
        TaxiStreamPipeline pipeline = new SpatialIndexPipeline();
        pipeline.buildPipeline();

        // Start the streaming pipeline.
        StreamingContext.getInstance().start();
        StreamingContext.getInstance().awaitTermination();
        StreamingContext.getInstance().stop(true, true);
    }
}
