package ch.ethz.gis;

import ch.ethz.gis.model.ClientRequest;
import ch.ethz.gis.model.Taxi;
import ch.ethz.gis.receiver.ClientRequestDStream;
import ch.ethz.gis.receiver.TaxiDStream;
import ch.ethz.gis.util.Util;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.RangePartitioner;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;
import scala.math.Ordering;
import scala.math.Ordering$;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class TaxiStreamProcessorBasePartition {
    private static int NUM_PARTITIONS = 10;
    private static int NUM_TAXIS = 4;

    public static void main(String[] args) throws InterruptedException {
        System.setProperty("hadoop.home.dir", "C:\\Programs\\Hadoop-adds");
        System.setProperty("spark.streaming.blockInterval", "500ms");

        // Log only ERROR messages
        StreamingContext.getInstance().sparkContext().setLogLevel("ERROR");

        

        // Start the streaming pipeline.
        StreamingContext.getInstance().start();
        StreamingContext.getInstance().awaitTermination();
        StreamingContext.getInstance().stop(true, true);
    }
}
