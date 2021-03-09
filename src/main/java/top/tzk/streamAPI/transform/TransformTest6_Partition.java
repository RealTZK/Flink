package top.tzk.streamAPI.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import top.tzk.streamAPI.beans.SensorReading;

/**
 * @Author: tianzhenkun
 * @Date: 2021/2/4
 * @Description:
 * @Modified By:
 */
public class TransformTest6_Partition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> streamSource = environment.readTextFile("flink/src/main/resources/sensor.txt");

        streamSource.print("input");

        // 1.shuffle
        DataStream<String> shuffle = streamSource.shuffle();
        shuffle.print("shuffle");

        // 2.keyBy
        DataStream<SensorReading> sensorReadings = streamSource.map(s -> {
            String[] split = s.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });
        KeyedStream<SensorReading, String> keyedStream = sensorReadings.keyBy(SensorReading::getId);
        keyedStream.print("keyBy");

        // 3.global
        streamSource.global().print("global");

        environment.execute();
    }
}
