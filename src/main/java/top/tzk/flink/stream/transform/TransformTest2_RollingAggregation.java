package top.tzk.flink.stream.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import top.tzk.flink.bean.SensorReading;

public class TransformTest2_RollingAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = environment.readTextFile("src/main/resources/sensor.csv");

        DataStream<SensorReading> sensorReading = dataSource.map(value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });

        // 分组
        KeyedStream<SensorReading, String> keyedSensorReading = sensorReading.keyBy(SensorReading::getId);

        // 滚动聚合
        DataStream<SensorReading> temperature = keyedSensorReading.maxBy("temperature");

        temperature.print();
        environment.execute();
    }
}
