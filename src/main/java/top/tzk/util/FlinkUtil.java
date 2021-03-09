package top.tzk.util;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import top.tzk.streamAPI.beans.SensorReading;

/**
 * @Author: tianzhenkun
 * @Date: 2021/2/5
 * @Description:
 * @Modified By:
 */
public class FlinkUtil {

    private static final StreamExecutionEnvironment ENVIRONMENT;
    private static final StreamTableEnvironment TABLE_ENVIRONMENT;

    static {
        ENVIRONMENT = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        TABLE_ENVIRONMENT = StreamTableEnvironment.create(ENVIRONMENT,blinkStreamSettings);
    }

    public static StreamExecutionEnvironment getStreamExecutionEnvironment(){
        return ENVIRONMENT;
    }
    public static StreamTableEnvironment getTableEnvironment(){
        return TABLE_ENVIRONMENT;
    }

    public static void setParallelism(int parallelism){
        ENVIRONMENT.setParallelism(parallelism);
    }

    public static DataStream<String> getSensorReadingsStrings(){
        return ENVIRONMENT.readTextFile("flink/src/main/resources/sensor.txt");
    }

    public static DataStream<SensorReading> getSensorReadings(DataStream<String> SensorReadingsStrings){
        return SensorReadingsStrings.map(s -> {
            String[] split = s.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });
    }

    public static void setStreamTimeCharacteristic(TimeCharacteristic timeCharacteristic){
        ENVIRONMENT.setStreamTimeCharacteristic(timeCharacteristic);
    }

    public static void execute(){
        try {
            ENVIRONMENT.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
