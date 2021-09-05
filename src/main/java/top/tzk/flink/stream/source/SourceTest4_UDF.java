package top.tzk.flink.stream.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import top.tzk.flink.bean.SensorReading;

public class SourceTest4_UDF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> dataStreamSource = environment.addSource(new MessageGenerator());
        dataStreamSource.print("udf");
        environment.execute();
    }
}
