import com.google.common.collect.Lists;
import es.upm.cc.SpeedRadar;
import es.upm.cc.events.PositionEvent;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SpeedRadarTests {

    private StreamExecutionEnvironment env;

    @Before
    public void createEnv() {
        env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    }

    @Test
    public void shouldDetectTwoOverSpeedEvents() throws Exception {

        PositionEventSink.values.clear();

        String[] data = new String[]{
                "30,1,91,0,3,1,52,100000",
                "60,1,90,0,3,1,52,101000",
                "90,1,99,0,3,1,52,102000"
        };

        SingleOutputStreamOperator<PositionEvent> source = Utils.streamFromLines(env, data);

        SpeedRadar.run(source).addSink(new PositionEventSink());
        env.execute();

        assertEquals(Lists.newArrayList(30, 90), PositionEventSink.values);
    }

    private static class PositionEventSink implements SinkFunction<PositionEvent> {

        // must be static
        static final List<Integer> values = new ArrayList<>();

        @Override
        public synchronized void invoke(PositionEvent event) throws Exception {
            values.add(event.getTimestamp());
        }
    }

}
