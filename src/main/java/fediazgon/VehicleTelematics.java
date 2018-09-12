package fediazgon;

import fediazgon.events.PositionEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class VehicleTelematics {

    public static final String SPEED_RADAR_FILE = "speedfines.csv";
    public static final String AVG_SPEED_FILE = "avgspeedfines.csv";
    public static final String ACCIDENTS_FILE = "accidents.csv";

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.out.println("Usage: <input file> <output folder>");
            throw new Exception();
        }

        String inputFile = args[0];
        String outputFolder = args[1];

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<String> linesSource = env.readTextFile(inputFile).setParallelism(1);
        SingleOutputStreamOperator<PositionEvent> mappedlines = linesSource.map(new Tokenizer());

        SpeedRadar.run(mappedlines)
                .writeAsCsv(String.format("%s/%s", outputFolder, SPEED_RADAR_FILE));

        AverageSpeedControl.run(mappedlines)
                .writeAsCsv(String.format("%s/%s", outputFolder, AVG_SPEED_FILE));

        AccidentReporter.run(linesSource.map(new Tokenizer()).setParallelism(1))
                .writeAsCsv(String.format("%s/%s", outputFolder, ACCIDENTS_FILE));

        env.execute("vehicle-telematics");

    }

    public static final class Tokenizer implements MapFunction<String, PositionEvent> {

        private static final long serialVersionUID = 1L;

        PositionEvent event = new PositionEvent();

        @Override
        public PositionEvent map(String s) throws Exception {
            String[] fields = s.split(",");
            int[] fieldsInt = Arrays.stream(fields).mapToInt(Integer::parseInt).toArray();
            event.setTime(fieldsInt[0]);
            event.setVid(String.valueOf(fieldsInt[1]));
            event.setSpeed(fieldsInt[2]);
            event.setHighway(fieldsInt[3]);
            event.setLane(fieldsInt[4]);
            event.setDirection(fieldsInt[5]);
            event.setSegment(fieldsInt[6]);
            event.setPosition(fieldsInt[7]);
            return event;
        }
    }

}


