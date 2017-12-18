package es.upm.cc.events;

import org.apache.flink.api.java.tuple.Tuple8;

public class PositionEvent extends Tuple8<Integer, String, Integer,
        Integer, Integer, Integer, Integer, Integer> {

    public PositionEvent() {
    }

    public PositionEvent(int timestamp, String vid, int speed, int xway,
                         int lane, int dir, int seg, int pos) {
        super(timestamp, vid, speed, xway, lane, dir, seg, pos);
    }

    public int getTimestamp() {
        return f0;
    }

    public String getVid() {
        return f1;
    }

    public int getSpeed() {
        return f2;
    }

    public int getXway() {
        return f3;
    }

    public int getDir() {
        return f5;
    }

    public int getSegment() {
        return f6;
    }

    public int getPosition() {
        return f7;
    }
}
