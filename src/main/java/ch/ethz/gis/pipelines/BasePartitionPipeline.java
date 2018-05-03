package ch.ethz.gis.pipelines;

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

/**
 * The idea of the BasePartitionPipeline is that all ClientRequests are collected on the driver / master, and this
 * bundle is sent to all workers (each handling a partition / share of all taxi data). This means that each worker
 * has to handle n/numWorkers taxis, guaranteeing some scalability (as long as the client requests don't reach
 * enormous numbers).
 * <p>
 * In any case, the stateful processing forces everything onto the driver, so collecting all ClientRequests there
 * is actually not such a problem.
 * <p>
 * This does not use the spatial properties of Taxis and ClientRequests.
 */
public class BasePartitionPipeline implements TaxiStreamPipeline {
    private static int NUM_PARTITIONS = 10;
    private static int NUM_TAXIS = 4;

    @Override
    public void buildPipeline() {
        // Create two (stateful) streams. The state computations happen on the driver / master.
        JavaPairDStream<Integer, Taxi> taxiStream = TaxiDStream.createStateful();
        JavaPairDStream<Integer, ClientRequest> clientRequestStream = ClientRequestDStream.createStateful();

        JavaPairDStream<ClientRequest, List<Tuple2<Taxi, Double>>> taxisForClientRequestStream =
                taxiStream.transformWithToPair(clientRequestStream, (taxiRdd, clientRdd, time) -> {
                    // Here, we collect all the client requests, so that we can make them available in the closure (snippet of
                    // code that is sent to the workers) below.
                    // This needs to happen on the driver, as we have to aggregate and re-distributed all client requests on a
                    // single machine anyways (well, technically, we could probably send all client requests from their partitions
                    // to all partitions of the taxis, but how to do that (if it's even possible) is not clear).
                    List<ClientRequest> clientRequests = clientRdd.values().collect();

                    // Now, we repartition. In this case, a range partitioner is used, but it could just as well be
                    // a hash partitioner. The range partitioner stems from an earlier approach. The default time partitioner
                    // would not work, however, as the statefulness of the stream destroys it (?).
                    final Ordering<Integer> ordering = Ordering$.MODULE$.comparatorToOrdering(Comparator.<Integer>naturalOrder());
                    final ClassTag<Integer> classTag = ClassTag$.MODULE$.apply(Integer.class);
                    final RangePartitioner partitioner = new RangePartitioner<>(NUM_PARTITIONS, taxiRdd.rdd(), true, ordering, classTag);

                    return taxiRdd.partitionBy(partitioner).mapPartitionsToPair(taxiPartition -> {
                        @SuppressWarnings("unchecked")
                        List<Tuple2<Integer, Taxi>> rawTaxis = IteratorUtils.toList(taxiPartition);
                        List<Taxi> taxis = rawTaxis.stream().map(Tuple2::_2).collect(Collectors.toList());

                        List<Tuple2<ClientRequest, List<Tuple2<Taxi, Double>>>> result = new ArrayList<>();
                        System.out.println("The number of ClientRequests on this partition is: " + clientRequests.size() +
                                ", and the number of Taxis is: " + taxis.size());

                        // We now simply iterate through all ClientRequests (on each worker) to get a list of taxis close to
                        // this ClientRequest (a list of size NUM_TAXIS).
                        for (ClientRequest c : clientRequests) {
                            List<Tuple2<Taxi, Double>> taxisForRequests = new ArrayList<>();
                            for (Taxi t : taxis) {
                                taxisForRequests.add(new Tuple2<>(t, Util.distance(t.getLon(), t.getLat(), c.getLon(), c.getLat())));
                            }
                            taxisForRequests.sort(Comparator.comparing(Tuple2::_2));
                            if (taxisForRequests.size() > NUM_TAXIS) {
                                result.add(new Tuple2<>(c, new ArrayList<>(taxisForRequests.subList(0, NUM_TAXIS))));
                            } else {
                                result.add(new Tuple2<>(c, taxisForRequests));
                            }
                        }

                        return result.iterator();
                    });
                });

        // Finally, we reduce everything to be on a single node again. This mainly serves the purpose that for each
        // ClientRequest we only have NUM_TAXIS candidates (instead of NUM_PARTITIONS * NUM_TAXIS before).
        // This can then be printed or further processed on the driver (also repartitioned again, etc.).
        JavaPairDStream<ClientRequest, List<Tuple2<Taxi, Double>>> reducedTaxisForClientRequestStream =
                taxisForClientRequestStream.reduceByKey((list1, list2) -> {
                    list1.addAll(list2);
                    list1.sort(Comparator.comparing(Tuple2::_2));
                    return list1;
                }).mapToPair(taxisForRequest -> {
                    if (taxisForRequest._2.size() > NUM_TAXIS) {
                        return new Tuple2<>(taxisForRequest._1, new ArrayList<>(taxisForRequest._2.subList(0, NUM_TAXIS)));
                    } else {
                        return new Tuple2<>(taxisForRequest._1, taxisForRequest._2);
                    }
                });

        reducedTaxisForClientRequestStream.print();
    }
}
