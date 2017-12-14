package es.upm.cc;

import es.upm.cc.events.AverageSpeedEvent;
import es.upm.cc.events.PositionEvent;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public final class AverageSpeedControl {

    public static SingleOutputStreamOperator<AverageSpeedEvent> run(SingleOutputStreamOperator<PositionEvent> stream) {
        return stream.filter((PositionEvent e) -> e.getSegment() >= 52 && e.getSegment() <= 56)
                .keyBy(new KeySelector<PositionEvent, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> getKey(PositionEvent positionEvent) throws Exception {
                        return new Tuple3<>(positionEvent.getVid(),
                                positionEvent.getXway(),
                                positionEvent.getDir());
                    }
                })
                .window(EventTimeSessionWindows.withGap(Time.seconds(30)))
                .apply(myWindowFunction);
    }

    private static WindowFunction<PositionEvent, AverageSpeedEvent, Tuple3<String, Integer, Integer>, TimeWindow> myWindowFunction =
            new WindowFunction<PositionEvent, AverageSpeedEvent, Tuple3<String, Integer, Integer>, TimeWindow>() {

                @Override
                public void apply(Tuple3<String, Integer, Integer> key, TimeWindow timeWindow, Iterable<PositionEvent> iterable, Collector<AverageSpeedEvent> collector) throws Exception {

                    boolean[] completedSegments = new boolean[5];
                    boolean completed = true;

                    int firstTimestamp = Integer.MAX_VALUE;
                    int lastTimestamp = 0;

                    int speedSum = 0;
                    int elements = 0;

                    for (PositionEvent e : iterable) {
                        speedSum += e.getSpeed();
                        int currentTimestamp = e.getTimestamp();
                        completedSegments[56 - e.getSegment()] = true;
                        firstTimestamp = Math.min(firstTimestamp, currentTimestamp);
                        lastTimestamp = Math.max(lastTimestamp, currentTimestamp);
                        elements++;
                    }

                    for (boolean completedSegment : completedSegments) {
                        if (!completedSegment) {
                            completed = false;
                            break;
                        }
                    }
                    if (completed)
                        collector.collect(new AverageSpeedEvent(firstTimestamp, lastTimestamp, key.f0,
                                42, 42, speedSum / elements));

                }
            };

}
