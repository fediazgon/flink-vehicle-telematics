package es.upm.cc;

import es.upm.cc.events.PositionEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.FileUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class Telematics {

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

        @Override
        public PositionEvent map(String s) throws Exception {
            String[] fields = s.split(",");
            int[] fieldsInt = Arrays.stream(fields).mapToInt(Integer::parseInt).toArray();
            return new PositionEvent(fieldsInt[0], String.valueOf(fieldsInt[1]), fieldsInt[2], fieldsInt[3],
                    fieldsInt[4], fieldsInt[5], fieldsInt[6], fieldsInt[7]);
        }
    }

}


