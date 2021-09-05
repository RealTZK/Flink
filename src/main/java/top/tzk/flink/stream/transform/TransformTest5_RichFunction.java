package top.tzk.flink.stream.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import top.tzk.flink.bean.SensorReading;

public class TransformTest5_RichFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = environment.readTextFile("src/main/resources/sensor.csv");

        DataStream<SensorReading> sensorReadings = dataSource.map(value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = sensorReadings.map(new RichMapFunction<SensorReading, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), getRuntimeContext().getIndexOfThisSubtask());
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                // 初始化,一般是定义状态,或者建立数据库连接
                System.out.println("open");
            }

            @Override
            public void close() throws Exception {
                // 一般是关闭连接和清理状态的收尾操作
                System.out.println("close");
            }
        });
        resultStream.print();

        environment.execute();
    }
}
