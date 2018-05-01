package ch.ethz.gis;

import ch.ethz.gis.model.ClientRequest;
import ch.ethz.gis.model.Taxi;
import ch.ethz.gis.receiver.ClientRequestDStream;
import ch.ethz.gis.receiver.TaxiDStream;
import ch.ethz.gis.util.Util;
import com.google.common.collect.Lists;
import org.apache.spark.RangePartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;
import scala.Tuple3;
import scala.math.Ordering;
import scala.math.Ordering$;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

public class TaxiStreamProcessor {
    public static void main(String[] args) throws InterruptedException {
        System.setProperty("hadoop.home.dir", "D:\\Programs\\hadoop");
        System.setProperty("spark.streaming.blockInterval", "500ms");

        // Log only ERROR messages
        StreamingContext.getInstance().sparkContext().setLogLevel("ERROR");

        JavaPairDStream<Integer, Taxi> taxiStream = TaxiDStream.createStateful();
        JavaPairDStream<Integer, ClientRequest> clientRequestStream = ClientRequestDStream.createStateful();

        JavaDStream<Tuple2<Integer, HashSet<Tuple3<Taxi, ClientRequest, Double>>>> repartitioned = taxiStream.transformWith(clientRequestStream, (taxis, clients, time) -> {
            JavaPairRDD<Double, Taxi> transformedTaxis = taxis.mapToPair(tuple -> new Tuple2<>(tuple._2.getLon(), tuple._2));
            JavaPairRDD<Double, ClientRequest> transformedClients = clients.mapToPair(tuple -> new Tuple2<>(tuple._2.getLon(), tuple._2));

            final Ordering<Double> ordering = Ordering$.MODULE$.comparatorToOrdering(Comparator.<Double>naturalOrder());
            final ClassTag<Double> classTag = ClassTag$.MODULE$.apply(Double.class);
            final RangePartitioner partitioner = new RangePartitioner<>(10, transformedTaxis.rdd(), true, ordering, classTag);

            JavaRDD<Tuple3<Taxi, ClientRequest, Double>> allRequestsForTaxis = transformedTaxis.partitionBy(partitioner)
                    .zipPartitions(transformedClients.partitionBy(partitioner), (taxiPartition, clientPartition) -> {
                        List<Tuple3<Taxi, ClientRequest, Double>> requestsForTaxi = new ArrayList<>();
                        ArrayList<Tuple2<Double, ClientRequest>> clientRequests = Lists.newArrayList(clientPartition);

                        while (taxiPartition.hasNext()) {
                            Taxi t = taxiPartition.next()._2;

                            for (Tuple2<Double, ClientRequest> c : clientRequests) {
                                requestsForTaxi.add(new Tuple3<>(t, c._2, Util.distance(t.getLon(), t.getLat(), c._2.getLon(), c._2.getLat())));
                            }
                        }
                        return requestsForTaxi.iterator();
                    });

            JavaPairRDD<Integer, HashSet<Tuple3<Taxi, ClientRequest, Double>>> result = allRequestsForTaxis.mapToPair((triple) -> new Tuple2<>(triple._1().getId(), triple))
                    .aggregateByKey(new HashSet<Tuple3<Taxi, ClientRequest, Double>>(), (set, req) -> {
                        set.add(req);
                        return set;
                    }, (set1, set2) -> {
                        set1.addAll(set2);
                        return set1;
                    })
                    .mapToPair((tuple) -> {
                        List<Tuple3<Taxi, ClientRequest, Double>> topK = new ArrayList<>(tuple._2);
                        topK.sort(Comparator.comparing(Tuple3::_3));
                        if (topK.size() > 2) {
                            return new Tuple2<>(tuple._1, new HashSet(topK.subList(0, 2)));
                        } else {
                            return new Tuple2<>(tuple._1, new HashSet<>(topK));
                        }
                    });

            return result.map((tuple) -> tuple);
        });

        repartitioned.print();

        StreamingContext.getInstance().start();
        StreamingContext.getInstance().awaitTermination();
        StreamingContext.getInstance().stop(true, true);
    }
}
