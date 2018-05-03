package ch.ethz.gis.pipelines;

import ch.ethz.gis.model.ClientRequest;
import ch.ethz.gis.model.Taxi;
import ch.ethz.gis.receiver.ClientRequestDStream;
import ch.ethz.gis.receiver.TaxiDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

public class CartesianPipeline implements TaxiStreamPipeline {
    @Override
    public void buildPipeline() {

        // Create two (stateful) streams. The state computations happen on the driver / master.
        JavaPairDStream<Integer, Taxi> taxiStream = TaxiDStream.createStateful();
        JavaPairDStream<Integer, ClientRequest> clientRequestStream = ClientRequestDStream.createStateful();
    }
}
