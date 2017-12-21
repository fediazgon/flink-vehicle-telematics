package master2017.flink;

import master2017.flink.events.AverageSpeedEvent;
import master2017.flink.events.PositionEvent;
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
                .filter((PositionEvent e) -> e.f6 >= 52 && e.f6 <= 56)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<PositionEvent>() {
                    @Override
                    public long extractAscendingTimestamp(PositionEvent positionEvent) {
                        return positionEvent.f0 * 1000;
                    }
                })
                .keyBy(1, 3, 5)
                .window(EventTimeSessionWindows.withGap(Time.seconds(31))) // WHY FLINK?!
                .apply(new AvgWindow());
    }

    @SuppressWarnings(value = "unchecked")
    static class AvgWindow implements WindowFunction<PositionEvent, AverageSpeedEvent, Tuple, TimeWindow> {

        private AverageSpeedEvent avgSpeedEvent = new AverageSpeedEvent();

        @Override
        public void apply(Tuple t, TimeWindow timeWindow,
                          Iterable<PositionEvent> iterable,
                          Collector<AverageSpeedEvent> collector) throws Exception {

            Tuple3<String, Integer, Integer> key = (Tuple3<String, Integer, Integer>) t;

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
                    avgSpeedEvent.f0 = time1;
                    avgSpeedEvent.f1 = time2;
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
