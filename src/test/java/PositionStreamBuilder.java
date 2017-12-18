import es.upm.cc.Telematics;
import es.upm.cc.events.PositionEvent;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PositionStreamBuilder {

    private StreamExecutionEnvironment env;
    private SingleOutputStreamOperator<PositionEvent> stream;

    public PositionStreamBuilder(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public PositionStreamBuilder fromLines(String[] lines) {
        stream = env.fromElements(lines).map(new Telematics.Tokenizer()).setParallelism(1);
        return this;
    }

    public SingleOutputStreamOperator<PositionEvent> build() {
        return stream;
    }


}

