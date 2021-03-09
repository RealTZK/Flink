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
public class TransformTest2_RollingAggregation {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStream<String> streamSource = environment.readTextFile("flink/src/main/resources/sensor.txt");

        DataStream<SensorReading> sensorReading = streamSource.map(s -> {
            String[] split = s.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });
        
        
        // 分组
        KeyedStream<SensorReading, String> keyedSensorReading = sensorReading.keyBy(SensorReading::getId);

        // 滚动聚合
        DataStream<SensorReading> temperature = keyedSensorReading.maxBy("temperature");

        temperature.print();

        environment.execute();

    }
}
