package es.upm.cc;

import es.upm.cc.events.AccidentEvent;
import es.upm.cc.events.PositionEvent;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class AccidentReporter {

    public static SingleOutputStreamOperator<AccidentEvent> run(SingleOutputStreamOperator<PositionEvent> stream) {
        return stream
                .filter(e -> e.getSpeed() == 0).setParallelism(1)
                .keyBy(new KeySelector<PositionEvent, Tuple5<String, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public Tuple5<String, Integer, Integer, Integer, Integer> getKey(PositionEvent positionEvent) throws Exception {
                        return new Tuple5<>(
                                positionEvent.getVid(),
                                positionEvent.getXway(),
                                positionEvent.getSegment(),
                                positionEvent.getDir(),
                                positionEvent.getPosition());
                    }
                }).countWindow(4, 1)
                .apply(new MyWindowFunction());
    }

    static class MyWindowFunction implements WindowFunction<PositionEvent, AccidentEvent,
            Tuple5<String, Integer, Integer, Integer, Integer>, GlobalWindow> {

        private AccidentEvent accidentEvent = new AccidentEvent();
        private PositionEvent lastElement = new PositionEvent();

        @Override
        public void apply(Tuple5<String, Integer, Integer, Integer, Integer> key, GlobalWindow globalWindow,
                          Iterable<PositionEvent> iterable, Collector<AccidentEvent> collector) throws Exception {

            Iterator<PositionEvent> events = iterable.iterator();

            int firstTimestamp = events.next().getTimestamp();
            int count = 1;

            while (events.hasNext()) {
                count++;
                lastElement = events.next();
            }

            if (count == 4) {
                accidentEvent.f0 = firstTimestamp;
                accidentEvent.f1 = lastElement.getTimestamp();
                accidentEvent.f2 = key.f0;
                accidentEvent.f3 = key.f1;
                accidentEvent.f4 = key.f2;
                accidentEvent.f5 = key.f3;
                accidentEvent.f6 = key.f4;
                collector.collect(accidentEvent);
            }
        }
    }

}
