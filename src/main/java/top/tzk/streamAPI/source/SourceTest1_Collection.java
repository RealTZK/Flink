package top.tzk.streamAPI.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import top.tzk.streamAPI.beans.SensorReading;

import java.util.Arrays;

/**
 * @Author: tianzhenkun
 * @Date: 2021/2/3
 * @Description:
 * @Modified By:
 */
public class SourceTest1_Collection {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从集合中读取数据
        DataStream<SensorReading> streamSource = environment.fromCollection(Arrays.asList(
                new SensorReading("1001", System.currentTimeMillis(), 36.6),
                new SensorReading("1007", System.currentTimeMillis(), 36.1),
                new SensorReading("1009", System.currentTimeMillis(), 37.6),
                new SensorReading("1020", System.currentTimeMillis(), 36.3),
                new SensorReading("1005", System.currentTimeMillis(), 35.6)
                )
        );

        DataStreamSource<Integer> integerDataStreamSource = environment.fromElements(1, 7, 5, 2341, 1235);

        streamSource.print("data");
        integerDataStreamSource.print("int");

        environment.execute("sourceAPI");

    }
}
