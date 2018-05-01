package ch.ethz.gis.receiver;

import ch.ethz.gis.StreamingContext;
import ch.ethz.gis.model.Taxi;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import scala.Tuple2;

import java.util.Objects;

public class TaxiDStream {
    public static JavaPairDStream<Integer, Taxi> createStateless() {
        JavaReceiverInputDStream<String> input = StreamingContext.getInstance().receiverStream(
                new WebSocketReceiver(StorageLevel.MEMORY_ONLY(), "http://localhost:8080/ws"));

        PairFunction<String, Integer, Taxi> f = (String in) -> {
            try {
                JsonNode j = new ObjectMapper().readTree(in);
                Taxi t = new Taxi(j.get("taxiId").asInt());
                t.setLon(j.get("lon").asDouble());
                t.setLat(j.get("lat").asDouble());
                return new Tuple2<>(t.getId(), t);
            } catch (Exception e) {
                return null;
            }
        };
        return input.mapToPair(f).filter(Objects::nonNull);
    }

    public static JavaPairDStream<Integer, Taxi> createStateful() {
        JavaPairDStream<Integer, Taxi> stateless = createStateless();

        Function3<Integer, Optional<Taxi>, State<Taxi>,
                Tuple2<Integer, Taxi>> f = (Integer id, Optional<Taxi> update, State<Taxi> state) -> {

            if (update.isPresent()) {
                state.update(update.get());
            }
            return new Tuple2<>(state.get().getId(), state.get());
        };
        return stateless.mapWithState(StateSpec.function(f)).stateSnapshots();
    }
}
