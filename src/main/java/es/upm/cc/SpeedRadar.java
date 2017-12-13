package es.upm.cc;

import es.upm.cc.events.PositionEvent;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public final class SpeedRadar {

    private SpeedRadar() {
    }

    private static final int MAXIMUM_SPEED = 90;

    public static SingleOutputStreamOperator<PositionEvent> run(SingleOutputStreamOperator<PositionEvent> stream) {
        return stream.filter((PositionEvent e) -> e.getSpeed() > MAXIMUM_SPEED);
    }

}
