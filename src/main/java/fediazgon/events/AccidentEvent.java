package fediazgon.events;

import org.apache.flink.api.java.tuple.Tuple7;

public class AccidentEvent extends Tuple7<Integer, Integer, String,
        Integer, Integer, Integer, Integer> {

    public AccidentEvent() {
    }

    public void setStartTime(int time) {
        f0 = time;
    }

    public void setFinishTime(int time) {
        f1 = time;
    }

    public void setVid(String vid) {
        f2 = vid;
    }

    public void setHighway(int highway) {
        f3 = highway;
    }

    public void setSegment(int segment) {
        f4 = segment;
    }

    public void setDirection(int direction) {
        f5 = direction;
    }

    public void setPosition(int position) {
        f6 = position;
    }

}
