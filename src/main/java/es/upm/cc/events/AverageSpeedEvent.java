package es.upm.cc.events;

import org.apache.flink.api.java.tuple.Tuple6;

public class AverageSpeedEvent extends Tuple6<Integer, Integer, String,
        Integer, Integer, Double> {

    public AverageSpeedEvent() {
    }

    public AverageSpeedEvent(int timestamp1, int timestamp2, String vid,
                             int xway, int dir, double avgSpeed) {
        super(timestamp1, timestamp2, vid, xway, dir, avgSpeed);
    }

}
