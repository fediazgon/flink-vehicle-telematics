package master2017.flink.events;

import org.apache.flink.api.java.tuple.Tuple8;

public class PositionEvent extends Tuple8<Integer, String, Integer,
        Integer, Integer, Integer, Integer, Integer> {

    public PositionEvent() {
    }

}
