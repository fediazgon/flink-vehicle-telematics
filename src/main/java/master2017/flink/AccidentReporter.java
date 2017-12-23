package master2017.flink;

import master2017.flink.events.AccidentEvent;
import master2017.flink.events.PositionEvent;
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
                .filter(e -> e.f2 == 0).setParallelism(1) // speed = 0
                .keyBy(new KeySelector<PositionEvent, Tuple5<String, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public Tuple5<String, Integer, Integer, Integer, Integer> getKey(PositionEvent positionEvent) throws Exception {
                        return new Tuple5<>(
                                positionEvent.f1,
                                positionEvent.f3,
                                positionEvent.f6,
                                positionEvent.f5,
                                positionEvent.f7);
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

            int time1 = events.next().f0;
            int count = 1;

            while (events.hasNext()) {
                count++;
                lastElement = events.next();
            }

            if (count == 4) {
                accidentEvent.setStartTime(time1);
                accidentEvent.setFinishTime(lastElement.f0);
                accidentEvent.setVid(key.f0);
                accidentEvent.setHighway(key.f1);
                accidentEvent.setSegment(key.f2);
                accidentEvent.setDirection(key.f3);
                accidentEvent.setPosition(key.f4);
                collector.collect(accidentEvent);
            }
        }
    }

}
