package master2017.flink;

import master2017.flink.events.PositionEvent;
import master2017.flink.events.SpeedEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public final class SpeedRadar {

    private static final int MAXIMUM_SPEED = 90;

    public static SingleOutputStreamOperator<SpeedEvent> run(SingleOutputStreamOperator<PositionEvent> stream) {
        return stream
                .filter((PositionEvent e) -> e.getSpeed() > MAXIMUM_SPEED)
                .map(new ToSpeedEvent());
    }


    @FunctionAnnotation.ForwardedFields({"f0", "f1", "f3->f2", "f6->f3", "f5->f4", "f2->f5"})
    public static class ToSpeedEvent implements MapFunction<PositionEvent, SpeedEvent> {

        SpeedEvent speedEvent = new SpeedEvent();

        @Override
        public SpeedEvent map(PositionEvent positionEvent) throws Exception {
            speedEvent.setTime(positionEvent.getTime());
            speedEvent.setVid(positionEvent.getVid());
            speedEvent.setHighway(positionEvent.getHighway());
            speedEvent.setSegment(positionEvent.getSegment());
            speedEvent.setDirection(positionEvent.getDirection());
            speedEvent.setSpeed(positionEvent.getSpeed());
            return speedEvent;
        }
    }

}
