import master2017.flink.AccidentReporter;
import master2017.flink.events.AccidentEvent;
import master2017.flink.events.PositionEvent;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class AccidentReporterTests extends StreamingMultipleProgramsTestBase {

    private StreamExecutionEnvironment env;
    SingleOutputStreamOperator<PositionEvent> source;

    private String[] data;

    @Before
    public void createEnv() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    }

    @After
    public void emptySink() {
        AccidentEventSink.values.clear();
    }

    @Test
    public void shouldDetectOneAccidentEvent() throws Exception {

        this.data = new String[]{
                "500,1,0,10,2,0,42,200000",
                "530,1,0,10,2,0,42,200000",
                "560,1,0,10,2,0,42,200000",
                "590,1,0,10,2,0,42,200000",
        };

        SingleOutputStreamOperator<PositionEvent> source =
                new PositionStreamBuilder(env)
                        .fromLines(data).withParallelism(1)
                        .build();

        AccidentReporter.run(source).addSink(new AccidentEventSink());
        env.execute();

        List<AccidentEvent> events = AccidentEventSink.values;

        assertEquals(1, AccidentEventSink.values.size());

        AccidentEvent first = events.get(0);
        assertEquals(500, (int) first.f0);
        assertEquals(590, (int) first.f1);
        assertEquals("1", first.f2);
        assertEquals(10, (int) first.f3);
        assertEquals(42, (int) first.f4);
        assertEquals(0, (int) first.f5);
        assertEquals(200000, (int) first.f6);

    }

    @Test
    public void shouldDetectThreeAccidentEvents() throws Exception {

        this.data = new String[]{
                "100,1,0,0,3,1,52,100000",
                "130,1,0,0,3,1,52,100000",
                "160,1,0,0,3,1,52,100000",
                "190,1,0,0,3,1,52,100000",
                "220,1,0,0,3,1,52,100000",
                "250,1,0,0,3,1,52,100000",
        };

        SingleOutputStreamOperator<PositionEvent> source =
                new PositionStreamBuilder(env)
                        .fromLines(data).withParallelism(1)
                        .build();

        AccidentReporter.run(source).addSink(new AccidentEventSink());
        env.execute();

        List<AccidentEvent> events = AccidentEventSink.values;

        assertEquals(3, AccidentEventSink.values.size());

        AccidentEvent first = events.get(0);
        assertEquals(100, (int) first.f0);
        assertEquals(190, (int) first.f1);

        AccidentEvent second = events.get(1);
        assertEquals(130, (int) second.f0);
        assertEquals(220, (int) second.f1);

        AccidentEvent third = events.get(2);
        assertEquals(160, (int) third.f0);
        assertEquals(250, (int) third.f1);
    }

    private static class AccidentEventSink implements SinkFunction<AccidentEvent> {

        // must be static
        static final List<AccidentEvent> values = new ArrayList<>();

        @Override
        public synchronized void invoke(AccidentEvent event) throws Exception {
            values.add(event);
        }
    }

}
