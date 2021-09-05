package top.tzk.flink.stream.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import top.tzk.flink.bean.SensorReading;

public class TransformTest3_Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = environment.readTextFile("src/main/resources/sensor.csv");

        DataStream<SensorReading> sensorReading = dataSource.map(value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });

        // 分组
        KeyedStream<SensorReading, String> keyedSensorReading = sensorReading.keyBy(SensorReading::getId);

        // reduce聚合,取最大的温度值,以及最新的时间戳
        DataStream<SensorReading> reduce = keyedSensorReading.reduce((lastValue, curValue) ->
                new SensorReading(
                        curValue.getId(),
                        curValue.getTimestamp(),
                        Math.max(curValue.getTemperature(), lastValue.getTemperature())));

        reduce.print();
        environment.execute();
    }
}
