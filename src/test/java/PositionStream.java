import es.upm.cc.Telematics;
import es.upm.cc.events.PositionEvent;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

public class PositionStream {

    private static StreamExecutionEnvironment env;

    static SingleOutputStreamOperator<PositionEvent> getStream() {
        ClassLoader classLoader = PositionStream.class.getClassLoader();
        String filePath = classLoader.getResource("test.csv").getPath();
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        return env.readTextFile(filePath)
                .map(new Telematics.Tokenizer())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<PositionEvent>() {
                    @Override
                    public long extractAscendingTimestamp(PositionEvent positionEvent) {
                        return positionEvent.getTimestamp() * 1000;
                    }
                });
    }

    static void startStreaming() {
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
