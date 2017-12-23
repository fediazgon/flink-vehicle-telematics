package master2017.flink.events;

import org.apache.flink.api.java.tuple.Tuple6;

public class AvgSpeedEvent extends Tuple6<Integer, Integer, String,
        Integer, Integer, Double> {

    public AvgSpeedEvent() {
    }

    public void setEntryTime(int time) {
        f0 = time;
    }

    public void setExitTime(int time) {
        f1 = time;
    }

    public void setVid(String vid) {
        f2 = vid;
    }

    public void setHighway(int highway) {
        f3 = highway;
    }

    public void setDirection(int direction) {
        f4 = direction;
    }

    public void setAvgSpeed(double avgSpeed) {
        f5 = avgSpeed;
    }

}
