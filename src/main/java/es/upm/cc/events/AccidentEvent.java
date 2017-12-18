package es.upm.cc.events;

import org.apache.flink.api.java.tuple.Tuple7;

public class AccidentEvent extends Tuple7<Integer, Integer, String,
        Integer, Integer, Integer, Integer> {

    public AccidentEvent() {
    }

    public AccidentEvent(int timestamp1, int timestamp2, String vid,
                         int xway, int seg, int dir, int pos) {
        super(timestamp1, timestamp2, vid, xway, seg, dir, pos);
    }

//    public int getT1() {
//        return f0;
//    }
//
//    public int getT2() {
//        return f1;
//    }

}
