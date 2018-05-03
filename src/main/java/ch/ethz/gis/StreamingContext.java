package ch.ethz.gis;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Milliseconds;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class StreamingContext {
    private static transient SparkSession sparkSession = null;
    private static JavaStreamingContext streamingContext = null;
    private static Logger logger = null;

    public static JavaStreamingContext getInstance() {
        // Set some Spark properties.
        System.setProperty("spark.streaming.blockInterval", "500ms");

        if (sparkSession == null) {
            sparkSession = SparkSession
                    .builder()
                    .appName("TaxiStreamProcessorBasePartition")
                    .master("local[*]")
                    .getOrCreate();
        }

        if (streamingContext == null) {
            JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
            streamingContext = new JavaStreamingContext(jsc, Milliseconds.apply(2000));
            streamingContext.checkpoint("checkpoint/");
        }
        return streamingContext;
    }

    public static Logger getLogger() {
        if (logger == null) {
            logger = Logger.getRootLogger();
        }
        return logger;
    }
}
