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

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AverageSpeedControlTests extends StreamingMultipleProgramsTestBase {

    private StreamExecutionEnvironment env;

    @Before
    public void createEnv() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    }

    @Test
    public void shouldNotDetectAvgSpeedEventWhenDoesNotGoThroughSegment() throws Exception {

        AverageSpeedEventSink.values.clear();

        String[] data = new String[]{
                "130,1,65,0,3,0,49,100000",
                "160,1,65,0,3,0,50,100900",
                "190,1,65,0,3,0,51,101800",
                "300,2,65,0,3,1,59,200000",
                "330,2,65,0,3,1,58,209000",
                "360,2,65,0,3,1,57,102700",
        };

        SingleOutputStreamOperator<PositionEvent> source
                = new PositionStreamBuilder(env).fromLines(data).build();

        AverageSpeedControl.run(source).addSink(new AverageSpeedEventSink());
        env.execute();

        assertTrue(AverageSpeedEventSink.values.isEmpty());
    }

    @Test
    public void shouldNotDetectAvgSpeedEventWhenSegmentUncompleted() throws Exception {

        AverageSpeedEventSink.values.clear();

        String[] data = new String[]{
                "100,1,65,0,3,0,52,100000",
                "130,1,65,0,3,0,53,100900",
                "160,1,65,0,3,0,54,101800",
                "190,1,65,0,3,0,55,102700",
        };

        SingleOutputStreamOperator<PositionEvent> source
                = new PositionStreamBuilder(env).fromLines(data).build();

        AverageSpeedControl.run(source).addSink(new AverageSpeedEventSink());
        env.execute();

        assertTrue(AverageSpeedEventSink.values.isEmpty());
    }

    @Test
    public void shouldNotDetectAvgSpeedEventWhenAverageIsBelowSixty() throws Exception {

        AverageSpeedEventSink.values.clear();

        String[] data = new String[]{
                "100,1,65,0,3,0,52,100000",
                "130,1,65,0,3,0,53,100100",
                "160,1,65,0,3,0,54,100200",
                "190,1,65,0,3,0,55,100300",
                "220,1,65,0,3,0,56,100400"
        };

        // 400 / 120 * 2.23694 = 7.46

        SingleOutputStreamOperator<PositionEvent> source
                = new PositionStreamBuilder(env).fromLines(data).build();

        AverageSpeedControl.run(source).addSink(new AverageSpeedEventSink());
        env.execute();

        assertTrue(AverageSpeedEventSink.values.isEmpty());
    }

    @Test
    public void shouldDetectOneAvgSpeedEventWhenSegmentCompleted() throws Exception {

        AverageSpeedEventSink.values.clear();

        String[] data = new String[]{
                "100,1,65,0,3,0,52,100000",
                "130,1,65,0,3,0,53,100900",
                "160,1,65,0,3,0,54,101800",
                "190,1,65,0,3,0,55,102700",
                "220,1,65,0,3,0,56,103600"
        };

        SingleOutputStreamOperator<PositionEvent> source
                = new PositionStreamBuilder(env).fromLines(data).build();

        AverageSpeedControl.run(source).addSink(new AverageSpeedEventSink());
        env.execute();

        Map<String, AverageSpeedEvent> events = AverageSpeedEventSink.values;
        assertEquals(1, events.size());

        AverageSpeedEvent e = events.get("1");
        assertEquals(100, e.getT1());
        assertEquals(220, e.getT2());
        assertEquals(67.1082, e.getAvgSpeed(), 0.0001);
    }

    @Test
    public void shouldDetectTwoAvgSpeedEvents() throws Exception {

        AverageSpeedEventSink.values.clear();

        String[] data = new String[]{
                "100,1,65,0,3,0,52,100000",
                "130,1,65,0,3,0,53,100900",
                "160,1,65,0,3,0,54,101800",
                "190,1,65,0,3,0,55,102700",
                "220,1,65,0,3,0,56,103600",
                "300,2,65,0,3,1,56,203600",
                "330,2,65,0,3,1,55,202700",
                "360,2,65,0,3,1,54,201800",
                "390,2,65,0,3,1,53,200900",
                "420,2,65,0,3,1,52,200000"
        };

        SingleOutputStreamOperator<PositionEvent> source
                = new PositionStreamBuilder(env).fromLines(data).build();

        AverageSpeedControl.run(source).addSink(new AverageSpeedEventSink());
        env.execute();

        Map<String, AverageSpeedEvent> events = AverageSpeedEventSink.values;
        assertEquals(2, events.size());

        AverageSpeedEvent first = events.get("1");
        assertEquals(100, first.getT1());
        assertEquals(220, first.getT2());
        assertEquals(67.1082, first.getAvgSpeed(), 0.0001);

        AverageSpeedEvent second = events.get("2");
        assertEquals(300, second.getT1());
        assertEquals(420, second.getT2());
        assertEquals(67.1082, second.getAvgSpeed(), 0.0001);
    }

    private static class AverageSpeedEventSink implements SinkFunction<AverageSpeedEvent> {

        // Hacky, because parallelism is not 1, we cannot assure that
        // events arrive in a particular order
        static final Map<String, AverageSpeedEvent> values = new HashMap<>();

        @Override
        public synchronized void invoke(AverageSpeedEvent event) throws Exception {
            values.put(event.f2, event);
        }
    }

}