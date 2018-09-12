package fediazgon;

import fediazgon.events.AvgSpeedEvent;
import fediazgon.events.PositionEvent;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public final class AverageSpeedControl {

    public static SingleOutputStreamOperator<AvgSpeedEvent> run(SingleOutputStreamOperator<PositionEvent> stream) {
        return stream
                .filter((PositionEvent e) -> e.getSegment() >= 52 && e.getSegment() <= 56)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<PositionEvent>() {
                    @Override
                    public long extractAscendingTimestamp(PositionEvent positionEvent) {
                        return positionEvent.getTime() * 1000;
                    }
                })
                .keyBy(new KeySelector<PositionEvent, AvgSpeedKey>() {

                    AvgSpeedKey key = new AvgSpeedKey();

                    @Override
                    public AvgSpeedKey getKey(PositionEvent positionEvent) throws Exception {
                        key.f0 = positionEvent.getVid();
                        key.f1 = positionEvent.getHighway();
                        key.f2 = positionEvent.getDirection();
                        return key;
                    }
                })
                .window(EventTimeSessionWindows.withGap(Time.seconds(31))) // WHY FLINK?!
                .apply(new AvgWindow());
    }

    private static class AvgSpeedKey extends Tuple3<String, Integer, Integer> {
    }

    @SuppressWarnings(value = "unchecked")
    private static class AvgWindow implements WindowFunction<PositionEvent, AvgSpeedEvent, AvgSpeedKey, TimeWindow> {

        private AvgSpeedEvent avgSpeedEvent = new AvgSpeedEvent();

        @Override
        public void apply(AvgSpeedKey key, TimeWindow timeWindow,
                          Iterable<PositionEvent> iterable,
                          Collector<AvgSpeedEvent> collector) throws Exception {

            byte completedSegments = 0;

            int time1 = Integer.MAX_VALUE;
            int pos1 = Integer.MAX_VALUE;

            int time2 = 0;
            int pos2 = 0;

            for (PositionEvent e : iterable) {
                int currentTime = e.f0;
                int currentPos = e.f7;
                completedSegments |= 1 << (56 - e.f6);
                time1 = Math.min(time1, currentTime);
                pos1 = Math.min(pos1, currentPos);
                time2 = Math.max(time2, currentTime);
                pos2 = Math.max(pos2, currentPos);
            }

            boolean completed = false;
            if (completedSegments == 0b11111)
                completed = true;

            if (completed) {
                double avgSpeed = (pos2 - pos1) * 1.0 / (time2 - time1) * 2.23694;
                if (avgSpeed > 60) {
                    avgSpeedEvent.setEntryTime(time1);
                    avgSpeedEvent.setExitTime(time2);
                    avgSpeedEvent.setVid(key.f0);
                    avgSpeedEvent.setHighway(key.f1);
                    avgSpeedEvent.setDirection(key.f2);
                    avgSpeedEvent.setAvgSpeed(avgSpeed);
                    collector.collect(avgSpeedEvent);
                }
            }
        }
    }

}
