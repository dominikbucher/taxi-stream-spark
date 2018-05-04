package ch.ethz.gis.pipelines;

import ch.ethz.gis.model.ClientRequest;
import ch.ethz.gis.model.Taxi;
import ch.ethz.gis.receiver.ClientRequestDStream;
import ch.ethz.gis.receiver.TaxiDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

/**
 * The idea here is to use the spatial properties of Taxis and ClientRequests. There are some options:
 *
 * <ul>
 * <li>
 * We build the indexes on each iteration, i.e., every batch. This has the downside that it is very likely
 * not too efficient, as we're not using the index for many kNN queries, and thus the index build time <i>might</i>
 * be longer than if we wouldn't build it. This needs testing though.
 * </li>
 * <li>
 * Similar to GeoSpark, we build up the indexes as RDDs, which are then joined to the Taxis, which update their
 * position in the RDDs. This needs stateful streams for the indexes as well, which forces them to the driver
 * at some point. The indexes are built for each partition of Taxis, so it will be required to partition the
 * Taxis based on their location. Forcing the spatial indexes to the driver might not be such a problem, as
 * the indexes are rather small, once created.
 * <p>
 * As for the overall (spatial) partitioner, e.g., a grid partitioner, it would be cool if this could be updated
 * occasionally (in case spatial Taxi distributions shift). Options: 1) Use a broadcast variable? 2) Build a timeout;
 * as the repartitioning happens on the driver anyways, we can simply build our own timeout there, and rebuild the
 * partitioner once it occurs. Then we maybe could have a boolean indicator that gets added to the closures sent to
 * the workers, which tells them they have to rebuild their indexes from scratch...
 * <p>
 * For this we need an updateable index. Need to search for that or implement it ourselves. I've seen people
 * doing research on it, and they claim its doable (simply remove old element, and insert new one).
 * </li>
 * <li>
 * We could try to leave the index on the workers, and only rebuild it once something goes wrong (or the worker gets
 * told to do so). I think it would be possible to simply have a variable pointing to a spatial index--this would be
 * local to the workers then. In a second step, we can simply check if the partitioner stayed the same (partition ID?)
 * and if the last partition was handled on this worker as well. This still feels a bit rough (and maybe not how Spark
 * is thought to be used, at least not basically--(ab)using locality is not forbidden but encouraged), but could
 * actually work.
 * </li>
 * </ul>
 */
public class SpatialIndexPipeline implements TaxiStreamPipeline {
    @Override
    public void buildPipeline() {
        // Create two (stateful) streams. The state computations happen on the driver / master.
        JavaPairDStream<Integer, Taxi> taxiStream = TaxiDStream.createStateful();
        JavaPairDStream<Integer, ClientRequest> clientRequestStream = ClientRequestDStream.createStateful();

        // TODO

    }
}
