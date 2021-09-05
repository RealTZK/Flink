package top.tzk.flink.stream.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import top.tzk.flink.bean.SensorReading;

import java.util.Arrays;

public class SourceTest1_Collection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> dataSource1 = environment.fromCollection(Arrays.asList(
                new SensorReading("1001", System.currentTimeMillis(), 36.6),
                new SensorReading("1009", System.currentTimeMillis(), 36.9),
                new SensorReading("1007", System.currentTimeMillis(), 38.6),
                new SensorReading("1003", System.currentTimeMillis(), 36.1),
                new SensorReading("1005", System.currentTimeMillis(), 37.0)
        ));

        DataStreamSource<Integer> dataSource2 = environment.fromElements(1, 7, 5, 1341, 1235);

        dataSource1.print("dataSource1");
        dataSource2.print("dataSource2");
        environment.execute();
    }
}
