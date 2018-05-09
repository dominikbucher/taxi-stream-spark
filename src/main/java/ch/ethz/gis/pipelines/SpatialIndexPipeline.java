package ch.ethz.gis.pipelines;

import ch.ethz.gis.StreamingContext;
import ch.ethz.gis.model.ClientRequest;
import ch.ethz.gis.model.Taxi;
import ch.ethz.gis.partitioner.GridPartitioner;
import ch.ethz.gis.partitioner.Point;
import ch.ethz.gis.partitioner.SpatialIndex;
import ch.ethz.gis.receiver.ClientRequestDStream;
import ch.ethz.gis.receiver.TaxiDStream;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Milliseconds;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Function1;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
public class SpatialIndexPipeline implements TaxiStreamPipeline, Serializable {
    private static int NUM_TAXIS = 4;

    @Override
    public void buildPipeline() {
        // We need to pass RDDs from one batch to the next (indexes).
        StreamingContext.getInstance().remember(Milliseconds.apply(4500));

        // Create two streams. The state computations happen on the driver / master. The taxis are stateless, as their
        // state is implicitly stored in the spatial index (if we'd use a state, the whole things would boil down to
        // rebuilding the spatial index from scratch for each batch, as in the stateful stream always all taxis are included).
        JavaPairDStream<Integer, Taxi> taxiStream = TaxiDStream.createStateless();
        JavaPairDStream<Point, ClientRequest> clientRequestStream = ClientRequestDStream.createStateful()
                .mapToPair((tuple) -> new Tuple2<>(tuple._2.getPoint(), tuple._2));

        // Create one overall GridPartitioner that divides space equally. We use the area of NYC as our extent, and
        // partition into 4 partitions. The extent is the same as in the Go application.
        GridPartitioner gridPartitioner = new GridPartitioner(-74.02, 40.82, -73.76, 40.61, 2, 2);

        // Not sure how optimal this is... but somehow we need to know which taxis are currently transitioning
        // from one partition to the other, so that we can remove them from their old spatial index.
        Function3<Integer, Optional<Tuple2<Taxi, Boolean>>, State<Tuple2<Taxi, Boolean>>,
                Tuple2<Integer, Tuple2<Taxi, Boolean>>> f = (Integer id, Optional<Tuple2<Taxi, Boolean>> update, State<Tuple2<Taxi, Boolean>> state) -> {

            Taxi newT = update.get()._1;
            if (state.exists()) {
                Taxi oldT = state.get()._1;

                if (gridPartitioner.getPartition(oldT.getPoint()) != gridPartitioner.getPartition(newT.getPoint())) {
                    state.update(new Tuple2<>(newT, true));
                    return new Tuple2<>(newT.getId(), new Tuple2<>(newT, true));
                }
            }
            state.update(new Tuple2<>(newT, false));
            return new Tuple2<>(newT.getId(), new Tuple2<>(newT, false));
        };
        JavaPairDStream<Integer, Boolean> transitioningTaxis = taxiStream
                .mapToPair(tuple -> new Tuple2<>(tuple._1, new Tuple2<>(tuple._2, false)))
                .mapWithState(StateSpec.function(f)).stateSnapshots()
                .filter(tuple -> tuple._2._2).mapToPair(tuple -> new Tuple2<>(tuple._1, tuple._2._2));

        // Repartition all streams (sadly, on the driver).
        JavaPairDStream<Point, Tuple2<Taxi, Optional<Boolean>>> repartitionedTaxiStream = taxiStream
                .leftOuterJoin(transitioningTaxis)
                .transformToPair(rdd -> rdd.mapToPair(tuple ->
                        new Tuple2<>(tuple._2._1.getPoint(), tuple._2)).partitionBy(gridPartitioner));
        JavaPairDStream<Point, ClientRequest> repartitionedClientRequestStream = clientRequestStream
                .transformToPair(rdd -> rdd.mapToPair(tuple ->
                        new Tuple2<>(tuple._2.getPoint(), tuple._2)).partitionBy(gridPartitioner));

        // Create a stream for the SpatialIndexes. They are updated with the taxis on each batch.
        // We're on the driver during transform operations anyways, so we can simply keep everything as a local variable.
        // We need the wrap, as the variable needs to be final to be passed to closures.
        List<SpatialIndex<Taxi>> indexes = gridPartitioner.createIndexes();
        JavaPairRDD<Point, SpatialIndex<Taxi>> spatialIndexRDD = StreamingContext.getInstance().sparkContext()
                .parallelize(indexes).mapToPair(index -> new Tuple2<>(index.getPoint(), index))
                .partitionBy(gridPartitioner);
        final RDDWrap indexesWrap = new RDDWrap(spatialIndexRDD);

        JavaDStream<Tuple2<SpatialIndex<Taxi>, List<Tuple2<ClientRequest, List<Taxi>>>>> zippedTaxisForClientRequests = repartitionedTaxiStream
                .transformWith(repartitionedClientRequestStream,
                        (taxiRdd, clientRequestRdd, time) -> {
                            final ClassTag<Tuple2<Point, SpatialIndex<Taxi>>> classTagSpatialIndex = ClassTag$.MODULE$.apply(Tuple2.class);
                            final ClassTag<Tuple2<SpatialIndex<Taxi>, List<Tuple2<ClientRequest, List<Taxi>>>>> classTagResult = ClassTag$.MODULE$.apply(Tuple2.class);

                            // Again, we need to collect all ClientRequests here, and pass them to the individual
                            // partitions. This is the same logic as in BasePartitionPipeline.
                            List<ClientRequest> clientRequests = clientRequestRdd.values().collect();
                            // Here, we simply collect all transitioning taxis.
                            Set<Taxi> transitioningTaxiList = new HashSet<>(taxiRdd.filter(tuple -> tuple._2._2.isPresent() && tuple._2._2.get())
                                    .map(tuple -> tuple._2._1).collect());

                            RDD<Tuple2<SpatialIndex<Taxi>, List<Tuple2<ClientRequest, List<Taxi>>>>> zippedPartitions = taxiRdd
                                    .map(tuple -> new Tuple2<>(tuple._1, tuple._2._1)).rdd()
                                    .zipPartitions(indexesWrap.indexRdd.rdd(), true,
                                            new MergeRDDsFunc(clientRequests, transitioningTaxiList), classTagSpatialIndex, classTagResult);

                            return new JavaRDD<>(zippedPartitions, classTagResult);
                        });

        zippedTaxisForClientRequests.cache();
        zippedTaxisForClientRequests.print();
        zippedTaxisForClientRequests.map(tuple -> tuple._1).foreachRDD(rdd -> {
            // Have to prune the lineage graph here. This is a bit annoying, but if we simply store the RDD, it will
            // also store all information about previous RDDs - leading to a StackOverflowError sooner or later.
            List<SpatialIndex<Taxi>> tmpIdxs = rdd.aggregate(new ArrayList<>(), (list, idx) -> {
                list.add(idx);
                return list;
            }, (list1, list2) -> {
                list1.addAll(list2);
                return list1;
            });

            indexesWrap.indexRdd.unpersist();
            //indexesWrap.indexRdd = rdd.mapToPair(tuple -> new Tuple2<>(tuple.getPoint(), tuple));
            //indexesWrap.indexRdd.cache();

            final ClassTag<SpatialIndex<Taxi>> classTag = ClassTag$.MODULE$.apply(SpatialIndex.class);
            JavaRDD<SpatialIndex<Taxi>> tempIndexRDD = new JavaRDD<>(rdd.context()
                    .parallelize(JavaConverters.collectionAsScalaIterableConverter(tmpIdxs).asScala().toSeq(),
                            gridPartitioner.numPartitions(), classTag), classTag);
            indexesWrap.indexRdd = tempIndexRDD.mapToPair(index -> new Tuple2<>(index.getPoint(), index))
                    .partitionBy(gridPartitioner);
        });
    }

    class MergeRDDsFunc extends scala.runtime.AbstractFunction2<Iterator<Tuple2<Point, Taxi>>, Iterator<Tuple2<Point, SpatialIndex<Taxi>>>,
            Iterator<Tuple2<SpatialIndex<Taxi>, List<Tuple2<ClientRequest, List<Taxi>>>>>> implements Serializable {

        private List<ClientRequest> clientRequests;
        private Set<Taxi> transitioningTaxiList;

        MergeRDDsFunc(List<ClientRequest> clientRequests, Set<Taxi> transitioningTaxiList) {
            this.clientRequests = clientRequests;
            this.transitioningTaxiList = transitioningTaxiList;
        }

        @Override
        public Iterator<Tuple2<SpatialIndex<Taxi>, List<Tuple2<ClientRequest, List<Taxi>>>>> apply(Iterator<Tuple2<Point, Taxi>> taxis,
                                                                                                   Iterator<Tuple2<Point, SpatialIndex<Taxi>>> indexes) {
            // This might not be the best way, but like this we can print out the number of Taxis, ClientRequests and
            // SpatialIndexes on each partition easily.
            List<Tuple2<Point, Taxi>> taxiList = JavaConversions.seqAsJavaList(taxis.toList());
            List<Tuple2<Point, SpatialIndex<Taxi>>> indexesList = JavaConversions.seqAsJavaList(indexes.toList());

            if (indexesList.size() == 0) {
                // This is okay, as the overflow partition doesn't have a spatial index. In that case, we should match
                // all with all again (or we adjust the grid partitioner).
                // FIXME For now it is left open how this partition keeps state... we cannot simply look at all the taxis
                // FIXME in this batch (the other partitions keep track via their Spatial Index).
                System.out.println("In overflow partition (#taxis: " + taxiList.size() + ", #clientRequests: " +
                        clientRequests.size() + ", #spatialIndexes: " + indexesList.size() + ")");
                return scala.collection.JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<SpatialIndex<Taxi>,
                        List<Tuple2<ClientRequest, List<Taxi>>>>>().iterator()).asScala();
            } else if (indexesList.size() > 1) {
                // More than one index should NEVER happen, however.
                System.err.println("Having more than one spatial index on a worker. This should not happen!");
                return scala.collection.JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<SpatialIndex<Taxi>,
                        List<Tuple2<ClientRequest, List<Taxi>>>>>().iterator()).asScala();
            } else {
                SpatialIndex<Taxi> i = indexesList.get(0)._2;
                List<Tuple2<ClientRequest, List<Taxi>>> taxiResults = new ArrayList<>();

                // Remove all taxis that are currently transitioning from one partition to another.
                for (Taxi t : transitioningTaxiList) {
                    i.remove(t);
                }

                for (Tuple2<Point, Taxi> taxi : taxiList) {
                    // Have to remove old taxi from the RTree first, and then add the new one (position).
                    Taxi t = taxi._2;
                    i.remove(t);
                    i.add(t);
                }
                System.out.println("In          partition (#taxis: " + taxiList.size() + ", #clientRequests: " +
                        clientRequests.size() + ", #spatialIndexes: " + indexesList.size() + ", #taxis in spatial index: " +
                        i.getrTree().size() + ", #unique taxis: " + taxiList.stream().map(taxi -> taxi._2.getId())
                        .collect(Collectors.toSet()).size() + ", #taxis to remove: " + transitioningTaxiList.size() + ")");

                // Finally, we can use the (updated) index to query for the kNN.
                for (ClientRequest cr : clientRequests) {
                    taxiResults.add(new Tuple2<>(cr, i.nearest(cr.getPoint(), 0.5, NUM_TAXIS)));
                }

                // Here we need to return everything we eventually need: The updated SpatialIndex, and
                // the Taxi/ClientRequest matches. The SpatialIndex will then update the one from the previous batch.
                List<Tuple2<SpatialIndex<Taxi>, List<Tuple2<ClientRequest, List<Taxi>>>>> results = new ArrayList<>();
                results.add(new Tuple2<>(i, taxiResults));

                return scala.collection.JavaConverters.asScalaIteratorConverter(results.iterator()).asScala();
            }
        }

        @Override
        public Function1<Iterator<Tuple2<Point, Taxi>>, Function1<Iterator<Tuple2<Point, SpatialIndex<Taxi>>>,
                Iterator<Tuple2<SpatialIndex<Taxi>, List<Tuple2<ClientRequest, List<Taxi>>>>>>> curried() {
            return null;
        }

        @Override
        public Function1<Tuple2<Iterator<Tuple2<Point, Taxi>>, Iterator<Tuple2<Point, SpatialIndex<Taxi>>>>,
                Iterator<Tuple2<SpatialIndex<Taxi>, List<Tuple2<ClientRequest, List<Taxi>>>>>> tupled() {
            return null;
        }
    }

    /**
     * We need this wrap as otherwise we couldn't update the indexRdd on each batch.
     */
    class RDDWrap implements Serializable {
        public JavaPairRDD<Point, SpatialIndex<Taxi>> indexRdd;

        RDDWrap(JavaPairRDD<Point, SpatialIndex<Taxi>> indexRdd) {
            this.indexRdd = indexRdd;
        }
    }
}
