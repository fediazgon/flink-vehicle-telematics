import es.upm.cc.AverageSpeedControl;
import es.upm.cc.events.AverageSpeedEvent;
import es.upm.cc.events.PositionEvent;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AverageSpeedControlTests extends StreamingMultipleProgramsTestBase {

    private StreamExecutionEnvironment env;

    @Before
    public void createEnv() {
        env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    }

    @Test
    public void shouldDetectOneAvgSpeedEvent() throws Exception {

        AverageSpeedEventSink.values.clear();

        String[] data = new String[]{
                "100,1,91,0,3,0,52,100000",
                "130,1,91,0,3,0,53,101000",
                "160,1,91,0,3,0,54,102000",
                "190,1,91,0,3,0,55,103000",
                "220,1,91,0,3,0,56,104000"
        };

        SingleOutputStreamOperator<PositionEvent> source = Utils.streamFromLines(env, data);

        AverageSpeedControl.run(source).addSink(new AverageSpeedEventSink());
        env.execute();

        List<AverageSpeedEvent> events = AverageSpeedEventSink.values;
        assertEquals(1, events.size());

        AverageSpeedEvent e = events.get(0);
        assertEquals(100, e.getT1());
        assertEquals(220, e.getT2());
        assertEquals(91, e.getAvgSpeed(), 0);
    }

    @Test
    public void shouldNotDetectAvgSpeedEvent() throws Exception {

        AverageSpeedEventSink.values.clear();

        String[] data = new String[]{
                "130,1,42,0,3,0,49,100000",
                "160,1,42,0,3,0,50,101000",
                "190,1,42,0,3,0,51,102000",
                "300,2,42,0,3,1,59,200000",
                "330,2,42,0,3,1,58,201000",
                "360,2,42,0,3,1,57,202000",
        };

        SingleOutputStreamOperator<PositionEvent> source = Utils.streamFromLines(env, data);

        AverageSpeedControl.run(source).addSink(new AverageSpeedEventSink());
        env.execute();

        assertTrue(AverageSpeedEventSink.values.isEmpty());
    }

    @Test
    public void shouldNotDetectAvgSpeedEventIncompleteSegment() throws Exception {

        AverageSpeedEventSink.values.clear();

        String[] data = new String[]{
                "100,1,91,0,3,0,52,100000",
                "130,1,91,0,3,0,53,101000",
                "160,1,91,0,3,0,54,102000",
                "190,1,91,0,3,0,55,103000",
        };

        SingleOutputStreamOperator<PositionEvent> source = Utils.streamFromLines(env, data);

        AverageSpeedControl.run(source).addSink(new AverageSpeedEventSink());
        env.execute();

        assertTrue(AverageSpeedEventSink.values.isEmpty());
    }

    @Test
    public void shouldDetectTwoAvgSpeedEventSameCar() throws Exception {

        AverageSpeedEventSink.values.clear();

        String[] data = new String[]{
                "100,1,91,0,3,0,52,100000",
                "130,1,91,0,3,0,53,101000",
                "160,1,91,0,3,0,54,102000",
                "190,1,91,0,3,0,55,103000",
                "220,1,91,0,3,0,56,104000",
                "300,1,40,0,3,1,56,204000",
                "330,1,50,0,3,1,55,203000",
                "360,1,60,0,3,1,54,202000",
                "390,1,50,0,3,1,53,201000",
                "420,1,40,0,3,1,52,200000"
        };

        SingleOutputStreamOperator<PositionEvent> source = Utils.streamFromLines(env, data);

        AverageSpeedControl.run(source).addSink(new AverageSpeedEventSink());
        env.execute();

        List<AverageSpeedEvent> events = AverageSpeedEventSink.values;
        assertEquals(2, events.size());

        AverageSpeedEvent first = events.get(0);
        assertEquals(100, first.getT1());
        assertEquals(220, first.getT2());
        assertEquals(91, first.getAvgSpeed(), 0);

        AverageSpeedEvent second = events.get(1);
        assertEquals(300, second.getT1());
        assertEquals(420, second.getT2());
        assertEquals(48, second.getAvgSpeed(), 0);
    }

    private static class AverageSpeedEventSink implements SinkFunction<AverageSpeedEvent> {

        // must be static
        static final List<AverageSpeedEvent> values = new ArrayList<>();

        @Override
        public synchronized void invoke(AverageSpeedEvent event) throws Exception {
            values.add(event);
        }
    }

}