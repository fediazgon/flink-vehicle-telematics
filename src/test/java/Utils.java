import es.upm.cc.Telematics;
import es.upm.cc.events.PositionEvent;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

public class Utils {

    public static SingleOutputStreamOperator<PositionEvent> streamFromLines
            (StreamExecutionEnvironment env, String[] lines) {
        return env.fromElements(lines)
                .map(new Telematics.Tokenizer())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<PositionEvent>() {
                    @Override
                    public long extractAscendingTimestamp(PositionEvent positionEvent) {
                        return positionEvent.getTimestamp() * 1000;
                    }
                });
    }

}
