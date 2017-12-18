package es.upm.cc;

import es.upm.cc.events.AccidentEvent;
import es.upm.cc.events.PositionEvent;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class AccidentReporter {

    public static SingleOutputStreamOperator<AccidentEvent> run(SingleOutputStreamOperator<PositionEvent> stream) {
        return stream
                .filter(e -> e.getSpeed() == 0).setParallelism(1)
                .keyBy(new KeySelector<PositionEvent, Tuple5<String, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public Tuple5<String, Integer, Integer, Integer, Integer> getKey(PositionEvent positionEvent) throws Exception {
                        return new Tuple5<>(positionEvent.getVid(),
                                positionEvent.getXway(),
                                positionEvent.getSegment(),
                                positionEvent.getDir(),
                                positionEvent.getPosition());
                    }
                }).countWindow(4, 1)
                .apply(myWindowFunction);
    }

    private static WindowFunction<PositionEvent, AccidentEvent, Tuple5<String, Integer, Integer, Integer, Integer>, GlobalWindow> myWindowFunction =
            new WindowFunction<PositionEvent, AccidentEvent, Tuple5<String, Integer, Integer, Integer, Integer>, GlobalWindow>() {

                @Override
                public void apply(Tuple5<String, Integer, Integer, Integer, Integer> key, GlobalWindow globalWindow, Iterable<PositionEvent> iterable, Collector<AccidentEvent> collector) throws Exception {

                    int firstTimestamp = Integer.MAX_VALUE;
                    int lastTimestamp = 0;
                    int count = 0;

                    for (PositionEvent e : iterable) {
                        int currentTimestamp = e.getTimestamp();
                        firstTimestamp = Math.min(firstTimestamp, currentTimestamp);
                        lastTimestamp = Math.max(lastTimestamp, currentTimestamp);
                        count++;
                    }

                    if (count == 4) {
                        collector.collect(new AccidentEvent(firstTimestamp, lastTimestamp,
                                key.f0, key.f1, key.f2, key.f3, key.f4));
                    }
                }
            };

}
