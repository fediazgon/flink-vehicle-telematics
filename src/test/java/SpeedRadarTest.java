import com.google.common.collect.Lists;
import es.upm.cc.SpeedRadar;
import es.upm.cc.events.PositionEvent;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SpeedRadarTest extends StreamingMultipleProgramsTestBase {

    private static SingleOutputStreamOperator<PositionEvent> source;

    @Before
    public void getStreamSource() {
        source = PositionStream.getStream();
    }

    @Test
    public void testOverSpeed() throws Exception {
        CollectSink.values.clear();

        SpeedRadar.run(source).addSink(new CollectSink());
        PositionStream.startStreaming();

        assertEquals(Lists.newArrayList(840, 870), CollectSink.values);
    }


    private static class CollectSink implements SinkFunction<PositionEvent> {

        // must be static
        static final List<Integer> values = new ArrayList<>();

        @Override
        public synchronized void invoke(PositionEvent event) throws Exception {
            values.add(event.getTimestamp());
        }
    }

}