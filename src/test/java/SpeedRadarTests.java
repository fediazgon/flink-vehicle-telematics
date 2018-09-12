import fediazgon.SpeedRadar;
import fediazgon.events.PositionEvent;
import fediazgon.events.SpeedEvent;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SpeedRadarTests extends AbstractTestBase {

    private StreamExecutionEnvironment env;

    @Before
    public void createEnv() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SpeedEventSink.values.clear();
    }

    @Test
    public void shouldDetectTwoOverSpeedEvents() throws Exception {

        String[] data = new String[]{
                "30,1,91,1,3,0,10,100000",
                "60,2,90,2,2,1,20,200000",
                "90,3,99,3,1,0,30,300000"
        };

        SingleOutputStreamOperator<PositionEvent> source
                = new PositionStreamBuilder(env).fromLines(data).build();

        SpeedRadar.run(source).addSink(new SpeedEventSink());
        env.execute();

        Map<String, SpeedEvent> events = SpeedEventSink.values;
        assertEquals(2, events.size());

        SpeedEvent first = events.get("1");
        assertEquals(30, first.f0.intValue());
        assertEquals(1, first.f2.intValue());
        assertEquals(10, first.f3.intValue());
        assertEquals(0, first.f4.intValue());
        assertEquals(91, first.f5.intValue());

        SpeedEvent second = events.get("3");
        assertEquals(90, second.f0.intValue());
        assertEquals(3, second.f2.intValue());
        assertEquals(30, second.f3.intValue());
        assertEquals(0, second.f4.intValue());
        assertEquals(99, second.f5.intValue());
    }

    @Test
    public void shouldNotDetectOverSpeedEvents() throws Exception {

        String[] data = new String[]{
                "230,100,90,2,4,1,52,400000",
                "260,200,89,2,4,1,53,500000",
                "290,300,88,2,4,1,54,600000"
        };

        SingleOutputStreamOperator<PositionEvent> source
                = new PositionStreamBuilder(env).fromLines(data).build();

        SpeedRadar.run(source).addSink(new SpeedEventSink());
        env.execute();

        Map<String, SpeedEvent> events = SpeedEventSink.values;
        assertTrue(events.isEmpty());

    }

    private static class SpeedEventSink implements SinkFunction<SpeedEvent> {

        // must be static
        static final Map<String, SpeedEvent> values = new HashMap<>();

        @Override
        public synchronized void invoke(SpeedEvent speedEvent) throws Exception {
            values.put(speedEvent.f1, speedEvent);
        }
    }

}
