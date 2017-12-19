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
                .window(EventTimeSessionWindows.withGap(Time.seconds(31)))
                .apply(new MyWindowFunction());
    }

    @SuppressWarnings(value = "unchecked")
    static class MyWindowFunction implements WindowFunction<PositionEvent, AverageSpeedEvent, Tuple, TimeWindow> {

        private AverageSpeedEvent avgSpeedEvent = new AverageSpeedEvent();

        @Override
        public void apply(Tuple t, TimeWindow timeWindow,
                          Iterable<PositionEvent> iterable,
                          Collector<AverageSpeedEvent> collector) throws Exception {

            Tuple3<String, Integer, Integer> key = (Tuple3<String, Integer, Integer>) t;

            byte completedSegments = 0;

            int firstTimestamp = Integer.MAX_VALUE;
            int firstPosition = Integer.MAX_VALUE;

            int lastTimestamp = 0;
            int lastPosition = 0;

            for (PositionEvent e : iterable) {
                int currentTimestamp = e.getTimestamp();
                int currentPosition = e.getPosition();
                completedSegments |= 1 << (56 - e.getSegment());
                firstTimestamp = Math.min(firstTimestamp, currentTimestamp);
                firstPosition = Math.min(firstPosition, currentPosition);
                lastTimestamp = Math.max(lastTimestamp, currentTimestamp);
                lastPosition = Math.max(lastPosition, currentPosition);
            }

            boolean completed = false;
            if (completedSegments == 0b11111)
                completed = true;

            if (completed) {
                double avgSpeed = (lastPosition - firstPosition) * 1.0 / (lastTimestamp - firstTimestamp) * 2.23694;
                if (avgSpeed > 60) {
                    avgSpeedEvent.f0 = firstTimestamp;
                    avgSpeedEvent.f1 = lastTimestamp;
                    avgSpeedEvent.f2 = key.f0;
                    avgSpeedEvent.f3 = key.f1;
                    avgSpeedEvent.f4 = key.f2;
                    avgSpeedEvent.f5 = avgSpeed;
                    collector.collect(avgSpeedEvent);
                }
            }
        }
    }

}
