package ch.ethz.gis.pipelines;

import ch.ethz.gis.StreamingContext;
import ch.ethz.gis.model.ClientRequest;
import ch.ethz.gis.model.Taxi;
import ch.ethz.gis.receiver.ClientRequestDStream;
import ch.ethz.gis.receiver.TaxiDStream;
import ch.ethz.gis.util.Util;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * The idea of the CartesianPipeline is simplicity. Match every taxi with every client and let Spark do the heavy
 * lifting by deciding how to distribute everything.
 *
 * This does not use the spatial properties of Taxis and ClientRequests.
 */
public class CartesianPipeline implements TaxiStreamPipeline {
    private static int NUM_TAXIS = 4;

    private synchronized int getBestNumberOfPartitions() {
        int workers = StreamingContext.getInstance().sparkContext().sc().getExecutorStorageStatus().length - 1;
        int parallelism = StreamingContext.getInstance().sparkContext().sc().defaultParallelism();
        ClassTag<Long> longTag = scala.reflect.ClassTag$.MODULE$.apply(Long.class);
        @SuppressWarnings("unchecked")
        JavaRDD<Long> testRdd = new JavaRDD(StreamingContext.getInstance().sparkContext().sc().range(0, 1, 1, parallelism), longTag);
        Integer cores = testRdd.map(v1 -> Runtime.getRuntime().availableProcessors()).collect().get(0);
        return workers * cores;
    }

    @Override
    public void buildPipeline() {
        int numberOfPartitions = Math.max(4, getBestNumberOfPartitions());
        System.out.println("Running with " + numberOfPartitions + " partitions.");

        // Create two (stateful) streams. The state computations happen on the driver / master.
        JavaPairDStream<Integer, Taxi> taxiStream = TaxiDStream.createStateful();
        JavaPairDStream<Integer, ClientRequest> clientRequestStream = ClientRequestDStream.createStateful();

        // Make the cartesian product, i.e., compute all pairs of Taxis and ClientRequests.
        JavaPairDStream<Taxi, ClientRequest> cartesianStream = taxiStream.transformWithToPair(clientRequestStream, (taxiRdd, clientRdd, time) -> {
            JavaPairRDD<Taxi, ClientRequest> cartesianRdd = taxiRdd.cartesian(clientRdd)
                    .mapToPair((taxiClientRequest) -> new Tuple2<>(taxiClientRequest._1._2, taxiClientRequest._2._2));
            return cartesianRdd.repartition(numberOfPartitions);
        });

        // Compute the distance between each Taxi and ClientRequest.
        JavaPairDStream<ClientRequest, Tuple2<Taxi, Double>> taxisForClientRequestComplete = cartesianStream.mapToPair((tuple) -> {
            ClientRequest c = tuple._2;
            Taxi t = tuple._1;
            return new Tuple2<>(c, new Tuple2<>(t, Util.distance(c.getLon(), c.getLat(), t.getLon(), t.getLat())));
        });

        // Reduce everything into a concise list of Taxis for each ClientRequest.
        JavaPairDStream<ClientRequest, ArrayList<Tuple2<Taxi, Double>>> taxisForClientRequest =
                taxisForClientRequestComplete.combineByKey(taxi -> {
                    ArrayList<Tuple2<Taxi, Double>> list = new ArrayList<>();
                    list.add(taxi);
                    return list;
                }, (list, taxi) -> {
                    list.add(taxi);
                    list.sort(Comparator.comparing(Tuple2::_2));
                    if (list.size() > NUM_TAXIS) {
                        return new ArrayList<>(list.subList(0, NUM_TAXIS));
                    } else {
                        return list;
                    }
                }, (list1, list2) -> {
                    list1.addAll(list2);
                    list1.sort(Comparator.comparing(Tuple2::_2));
                    if (list1.size() > NUM_TAXIS) {
                        return new ArrayList<>(list1.subList(0, NUM_TAXIS));
                    } else {
                        return list1;
                    }
                }, new HashPartitioner(numberOfPartitions));

        taxisForClientRequest.print();
    }
}
