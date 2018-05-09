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
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Milliseconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Function1;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

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
    @Override
    public void buildPipeline() {
        // We need to pass RDDs from one batch to the next (indexes).
        StreamingContext.getInstance().remember(Milliseconds.apply(4500));

        // Create two (stateful) streams. The state computations happen on the driver / master.
        JavaPairDStream<Integer, Taxi> taxiStream = TaxiDStream.createStateful();
        JavaPairDStream<Point, ClientRequest> clientRequestStream = ClientRequestDStream.createStateful()
                .mapToPair((tuple) -> new Tuple2<>(tuple._2.getPoint(), tuple._2));

        // Create one overall GridPartitioner that divides space equally. We use the area of NYC as our extent, and
        // partition into 4 partitions. The extent is the same as in the Go application.
        GridPartitioner gridPartitioner = new GridPartitioner(-74.02, 40.82, -73.76, 40.61, 2, 2);

        // Repartition all streams (sadly, on the driver).
        JavaPairDStream<Point, Taxi> repartitionedTaxiStream = taxiStream
                .transformToPair(rdd -> rdd.mapToPair(tuple ->
                        new Tuple2<>(tuple._2.getPoint(), tuple._2)).partitionBy(gridPartitioner));
        JavaPairDStream<Point, ClientRequest> repartitionedClientRequestStream = clientRequestStream
                .transformToPair(rdd -> rdd.mapToPair(tuple ->
                        new Tuple2<>(tuple._2.getPoint(), tuple._2)).partitionBy(gridPartitioner));

        // Create a stream for the SpatialIndexes. They are updated with the taxis on each batch.
        // We're on the driver during transform operations anyways, so we can simply keep everything as a local
        List<SpatialIndex> indexes = gridPartitioner.createIndexes();
        JavaPairRDD<Point, SpatialIndex> spatialIndexRDD = StreamingContext.getInstance().sparkContext()
                .parallelize(indexes).mapToPair(index -> new Tuple2<>(index.getPoint(), index))
                .partitionBy(gridPartitioner);
        final RDDWrap indexesWrap = new RDDWrap(spatialIndexRDD);

        // We could transform it into a stream like this, but I'm not sure if that helps us in any way here (resp. if
        // it's possible to achieve the same functionality with a stream).
        // Queue<JavaRDD<SpatialIndex>> spatialIndexQueue = new LinkedList<>();
        // spatialIndexQueue.add(spatialIndexRDD);
        // JavaPairDStream<Point, SpatialIndex> indexStream =
        //         StreamingContext.getInstance().queueStream(spatialIndexQueue).mapToPair((index) -> new Tuple2<>(index.getPoint(), index));

        JavaDStream<Tuple2<SpatialIndex, List<Tuple2<Point, Taxi>>>> zippedTaxisForClientRequests = repartitionedTaxiStream
                .transformWith(repartitionedClientRequestStream,
                        (taxiRdd, clientRequestRdd, time) -> {
                            final ClassTag<Tuple2<Point, ClientRequest>> classTag1 = ClassTag$.MODULE$.apply(Tuple2.class);
                            final ClassTag<Tuple2<Point, SpatialIndex>> classTag2 = ClassTag$.MODULE$.apply(Tuple2.class);
                            final ClassTag<Tuple2<SpatialIndex, List<Tuple2<Point, Taxi>>>> classTag3 = ClassTag$.MODULE$.apply(Tuple2.class);

                            RDD<Tuple2<SpatialIndex, List<Tuple2<Point, Taxi>>>> zippedPartitions = taxiRdd.rdd()
                                    .zipPartitions(clientRequestRdd.rdd(), indexesWrap.indexRdd.rdd(),
                                            true, new MergeRDDsFunc(), classTag1, classTag2, classTag3);

                            return new JavaRDD<>(zippedPartitions, classTag3);
                        });

        zippedTaxisForClientRequests.cache();
        zippedTaxisForClientRequests.print();
        zippedTaxisForClientRequests.map(tuple -> tuple._1).foreachRDD(rdd -> {
            // Have to prune the lineage graph here. This is a bit annoying, but if we simply store the RDD, it will
            // also store all information about previous RDDs - leading to a StackOverflowError sooner or later.
            List<SpatialIndex> tmpIdxs = rdd.aggregate(new ArrayList<>(), (list, idx) -> {
                list.add(idx);
                return list;
            }, (list1, list2) -> {
                list1.addAll(list2);
                return list1;
            });

            indexesWrap.indexRdd.unpersist();
            //indexesWrap.indexRdd = rdd.mapToPair(tuple -> new Tuple2<>(tuple.getPoint(), tuple));
            //indexesWrap.indexRdd.cache();

            final ClassTag<SpatialIndex> classTag = ClassTag$.MODULE$.apply(SpatialIndex.class);
            JavaRDD<SpatialIndex> tempIndexRDD = new JavaRDD<>(rdd.context()
                    .parallelize(JavaConverters.collectionAsScalaIterableConverter(tmpIdxs).asScala().toSeq(),
                            gridPartitioner.numPartitions(), classTag), classTag);
            indexesWrap.indexRdd = tempIndexRDD.mapToPair(index -> new Tuple2<>(index.getPoint(), index))
                    .partitionBy(gridPartitioner);
        });
    }

    class MergeRDDsFunc implements scala.Function3<Iterator<Tuple2<Point, Taxi>>,
            Iterator<Tuple2<Point, ClientRequest>>, Iterator<Tuple2<Point, SpatialIndex>>,
            Iterator<Tuple2<SpatialIndex, List<Tuple2<Point, Taxi>>>>>, Serializable {
        @Override
        public Iterator<Tuple2<SpatialIndex, List<Tuple2<Point, Taxi>>>> apply(Iterator<Tuple2<Point, Taxi>> taxis,
                                                                               Iterator<Tuple2<Point, ClientRequest>> clientRequests,
                                                                               Iterator<Tuple2<Point, SpatialIndex>> indexes) {
            // This might not be the best way, but like this we can print out the number of Taxis, ClientRequests and
            // SpatialIndexes on each partition easily.
            List<Tuple2<Point, Taxi>> taxiList = JavaConversions.seqAsJavaList(taxis.toList());
            List<Tuple2<Point, ClientRequest>> clientRequestsList = JavaConversions.seqAsJavaList(clientRequests.toList());
            List<Tuple2<Point, SpatialIndex>> indexesList = JavaConversions.seqAsJavaList(indexes.toList());

            System.out.println("Working on one partition (#taxis: " + taxiList.size() + ", #clientRequests: " +
                    clientRequestsList.size() + ", #spatialIndexes: " + indexesList.size() + ")");
            if (indexesList.size() > 0) {
                List<Tuple2<SpatialIndex, List<Tuple2<Point, Taxi>>>> results = new ArrayList<>();

                for (Tuple2<Point, SpatialIndex> tuple : indexesList) {
                    SpatialIndex i = tuple._2;
                    List<Tuple2<Point, Taxi>> taxiResults = new ArrayList<>();

                    // System.out.println(i.getrTree().size());
                    if (indexesList.size() > 1) {
                        System.err.println("Having more than one spatial index on a worker. This should not happen!");
                    }

                    for (Tuple2<Point, Taxi> taxi : taxiList) {
                        // Have to remove old taxi from the RTree first, and then add the new one (position).
                        Taxi t = taxi._2;
                        taxiResults.add(new Tuple2<>(t.getPoint(), t));
                    }

                    // Here we need to return everything we eventually need: The updated SpatialIndex, and
                    // the Taxi/ClientRequest matches. The SpatialIndex will then update the one from the previous batch.
                    results.add(new Tuple2<>(i, taxiResults));
                }

                return scala.collection.JavaConverters.asScalaIteratorConverter(results.iterator()).asScala();
            } else {
                return scala.collection.JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<SpatialIndex,
                        List<Tuple2<Point, Taxi>>>>().iterator()).asScala();
            }
        }

        @Override
        public Function1<Iterator<Tuple2<Point, Taxi>>,
                Function1<Iterator<Tuple2<Point, ClientRequest>>,
                        Function1<Iterator<Tuple2<Point, SpatialIndex>>,
                                Iterator<Tuple2<SpatialIndex, List<Tuple2<Point, Taxi>>>>>>> curried() {
            return null;
        }

        @Override
        public Function1<Tuple3<Iterator<Tuple2<Point, Taxi>>,
                Iterator<Tuple2<Point, ClientRequest>>,
                Iterator<Tuple2<Point, SpatialIndex>>>,
                Iterator<Tuple2<SpatialIndex, List<Tuple2<Point, Taxi>>>>> tupled() {
            return null;
        }
    }

    /**
     * We need this wrap as otherwise we couldn't update the indexRdd on each batch.
     */
    class RDDWrap implements Serializable {
        public JavaPairRDD<Point, SpatialIndex> indexRdd;

        RDDWrap(JavaPairRDD<Point, SpatialIndex> indexRdd) {
            this.indexRdd = indexRdd;
        }
    }
}
