package master2017.flink.events;

import org.apache.flink.api.java.tuple.Tuple7;

public class AccidentEvent extends Tuple7<Integer, Integer, String,
        Integer, Integer, Integer, Integer> {

    public AccidentEvent() {
    }

}
