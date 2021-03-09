package top.tzk.streamAPI.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import top.tzk.streamAPI.beans.SensorReading;

/**
 * @Author: tianzhenkun
 * @Date: 2021/2/4
 * @Description:
 * @Modified By:
 */
public class TransformTest5_RichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> streamSource = environment.readTextFile("flink/src/main/resources/sensor.txt");

        DataStream<SensorReading> sensorReadings = streamSource.map(s -> {
            String[] split = s.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });

        /*DataStream<Tuple2<String, Integer>> resultStream = sensorReadings.map(sensorReading->
                new Tuple2<>(sensorReading.getId(),sensorReading.getId().length())
        ).returns(Types.TUPLE(Types.STRING,Types.INT));*/

        DataStream<Tuple2<String, Integer>> resultStream = sensorReadings.map(new RichMapFunction<SensorReading, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(SensorReading sensorReading) {
//                getRuntimeContext().getState();
                return new Tuple2<>(sensorReading.getId(), getRuntimeContext().getIndexOfThisSubtask());
            }
            @Override
            public void open(Configuration parameters) {
                // 初始化,一般是定义状态,或者建立数据库连接
                System.out.println("open");
            }
            @Override
            public void close() {
                // 一般是关闭连接和清理状态的收尾操作
                System.out.println("close");
            }
        });

        resultStream.print();

        environment.execute();

    }
}
