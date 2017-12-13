import com.google.common.collect.Lists;
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
                "130,1,91,0,3,0,53,100000",
                "160,1,91,0,3,0,54,100000",
                "190,1,91,0,3,0,55,100000",
                "220,1,91,0,3,0,56,100000"
        };

        SingleOutputStreamOperator<PositionEvent> source = Utils.streamFromLines(env, data);

        AverageSpeedControl.run(source).addSink(new AverageSpeedEventSink());
        env.execute();

        assertEquals(Lists.newArrayList(100, 220),
                AverageSpeedEventSink.values);
    }

    @Test
    public void shouldNotDetectAvgSpeedEvent() throws Exception {

        AverageSpeedEventSink.values.clear();

        String[] data = new String[]{
                "100,1,91,0,3,0,52,100000",
                "130,1,91,0,3,0,53,100000",
                "160,1,91,0,3,0,54,100000",
                "190,1,91,0,3,0,55,100000",
        };

        SingleOutputStreamOperator<PositionEvent> source = Utils.streamFromLines(env, data);

        AverageSpeedControl.run(source).addSink(new AverageSpeedEventSink());
        env.execute();

        assertTrue(AverageSpeedEventSink.values.isEmpty());
    }

    private static class AverageSpeedEventSink implements SinkFunction<AverageSpeedEvent> {

        // must be static
        static final List<Integer> values = new ArrayList<>();

        @Override
        public synchronized void invoke(AverageSpeedEvent event) throws Exception {
            values.add(event.getT1());
            values.add(event.getT2());
        }
    }

}