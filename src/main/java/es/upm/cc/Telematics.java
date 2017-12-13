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

    private static Logger log = Logger.getLogger(Telematics.class);

    public static final String SPEED_RADAR_FILE = "speedfines.csv";
    public static final String AVG_SPEED_FILE = "avgspeedfines.csv";
    public static final String ACCIDENTS_FILE = "accidents.csv";

    public static void main(String[] args) {

        if (args.length < 2) {
            System.out.println("Usage: <input file> <output folder>");
            System.exit(1);
        }

        String inputFile = args[0];
        String outputFolder = args[1];

        //TODO: delete in the final version. Only here for testing.
        try {
            FileUtils.deleteFileOrDirectory((new File(outputFolder)));
        } catch (IOException e) {
            log.error(String.format("IOException: dir %s could no be cleaned", outputFolder));
            e.printStackTrace();
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().disableSysoutLogging();

        SingleOutputStreamOperator<PositionEvent> source = env.readTextFile(inputFile)
                .map(new Tokenizer())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<PositionEvent>() {
                    @Override
                    public long extractAscendingTimestamp(PositionEvent positionEvent) {
                        return positionEvent.getTimestamp() * 1000;
                    }
                });


        SpeedRadar.run(source)
                .writeAsCsv(String.format("%s/%s", outputFolder, SPEED_RADAR_FILE));

        AverageSpeedControl.run(source)
                .writeAsCsv(String.format("%s/%s", outputFolder, AVG_SPEED_FILE));

        try {
            env.execute("vehicle-telematics");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

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


