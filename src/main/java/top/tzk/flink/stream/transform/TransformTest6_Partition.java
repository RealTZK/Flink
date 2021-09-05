package top.tzk.flink.stream.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import top.tzk.flink.bean.SensorReading;

public class TransformTest6_Partition {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = environment.readTextFile("src/main/resources/sensor.csv");
        dataSource.print("input");

        // shuffle
        DataStream<String> shuffle = dataSource.shuffle();
        shuffle.print("shuffle");

        // keyBy
        KeyedStream<SensorReading, String> keyBy = dataSource.map(value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        }).keyBy(SensorReading::getId);
        keyBy.print("keyBy");

        // global
        dataSource.global().print("global");

        environment.execute();
    }
}
