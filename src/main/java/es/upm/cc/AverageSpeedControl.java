package es.upm.cc;

import es.upm.cc.events.AverageSpeedEvent;
import es.upm.cc.events.PositionEvent;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public final class AverageSpeedControl {

    public static SingleOutputStreamOperator<AverageSpeedEvent> run(SingleOutputStreamOperator<PositionEvent> stream) {
        return stream
                .filter((PositionEvent e) -> e.getSegment() >= 52 && e.getSegment() <= 56)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<PositionEvent>() {
                    @Override
                    public long extractAscendingTimestamp(PositionEvent positionEvent) {
                        return positionEvent.getTimestamp() * 1000;
                    }
                })
                .keyBy(1, 3, 5)
                .window(EventTimeSessionWindows.withGap(Time.seconds(30)))
                .apply(myWindowFunction);
    }

    @SuppressWarnings(value = "unchecked")
    private static WindowFunction<PositionEvent, AverageSpeedEvent, Tuple, TimeWindow> myWindowFunction =
            new WindowFunction<PositionEvent, AverageSpeedEvent, Tuple, TimeWindow>() {

                @Override
                public void apply(Tuple t, TimeWindow timeWindow,
                                  Iterable<PositionEvent> iterable,
                                  Collector<AverageSpeedEvent> collector) throws Exception {

                    Tuple3<String, Integer, Integer> key = (Tuple3<String, Integer, Integer>) t;

                    boolean[] completedSegments = new boolean[5];
                    boolean completed = true;

                    int firstTimestamp = Integer.MAX_VALUE;
                    int firstPosition = Integer.MAX_VALUE;

                    int lastTimestamp = 0;
                    int lastPosition = 0;

                    for (PositionEvent e : iterable) {
                        int currentTimestamp = e.getTimestamp();
                        int currentPosition = e.getPosition();
                        completedSegments[56 - e.getSegment()] = true;
                        firstTimestamp = Math.min(firstTimestamp, currentTimestamp);
                        firstPosition = Math.min(firstPosition, currentPosition);
                        lastTimestamp = Math.max(lastTimestamp, currentTimestamp);
                        lastPosition = Math.max(lastPosition, currentPosition);
                    }

                    for (boolean completedSegment : completedSegments) {
                        if (!completedSegment) {
                            completed = false;
                            break;
                        }
                    }
                    if (completed) {
                        double avgSpeed = (lastPosition - firstPosition) * 1.0 / (lastTimestamp - firstTimestamp) * 2.23694;
                        if (avgSpeed > 60)
                            collector.collect(new AverageSpeedEvent(firstTimestamp, lastTimestamp, key.f0,
                                    key.f1, key.f2, avgSpeed));

                    }
                }
            };

}
