import master2017.flink.VehicleTelematics;
import master2017.flink.events.PositionEvent;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PositionStreamBuilder {

    private StreamExecutionEnvironment env;
    private SingleOutputStreamOperator<PositionEvent> stream;

    public PositionStreamBuilder(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public PositionStreamBuilder fromLines(String[] lines) {
        stream = env.fromElements(lines)
                .setParallelism(1)
                .map(new VehicleTelematics.Tokenizer());
        return this;
    }

    public PositionStreamBuilder withParallelism(int parallelism) {
        stream = stream.setParallelism(1);
        return this;
    }

    public SingleOutputStreamOperator<PositionEvent> build() {
        return stream;
    }


}

