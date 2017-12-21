package master2017.flink.events;

import org.apache.flink.api.java.tuple.Tuple6;

public class AverageSpeedEvent extends Tuple6<Integer, Integer, String,
        Integer, Integer, Double> {

    public AverageSpeedEvent() {
    }

}
