package ch.ethz.gis;

import ch.ethz.gis.pipelines.CartesianPipeline;
import ch.ethz.gis.pipelines.TaxiStreamPipeline;

/**
 * Main class that runs the processing pipeline.
 */
public class TaxiStreamProcessor {
    public static void main(String[] args) throws InterruptedException {
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
}
