package top.tzk.streamAPI.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import top.tzk.streamAPI.beans.SensorReading;

import java.util.Collections;

/**
 * @Author: tianzhenkun
 * @Date: 2021/2/4
 * @Description:
 * @Modified By:
 */
public class TransformTest4_MultipleStreams {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStream<String> streamSource = environment.readTextFile("flink/src/main/resources/sensor.txt");

        DataStream<SensorReading> sensorReadings = streamSource.map(s -> {
            String[] split = s.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });

        // 分流,按照温度值37为界分为两条流
        SplitStream<SensorReading> splitStream = sensorReadings.split(sensorReading -> sensorReading.getTemperature() > 37 ?
                Collections.singletonList("high") : Collections.singleton("low")
        );

        DataStream<SensorReading> highTempStream = splitStream.select("high");
        DataStream<SensorReading> lowTempStream = splitStream.select("low");
        DataStream<SensorReading> allTempStream = splitStream.select("low","high");

        highTempStream.print("high");
        lowTempStream.print("low");
        allTempStream.print("all");

        // 2.合流 connect,将高温流转换成二元组类型,与低温流连接合并之后,输出状态信息
        DataStream<Tuple2<String, Double>> warningStream = highTempStream.map((MapFunction<SensorReading, Tuple2<String, Double>>) sensorReading ->
                new Tuple2<>(sensorReading.getId(), sensorReading.getTemperature())).returns(Types.TUPLE(Types.STRING,Types.DOUBLE));

        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStreams = warningStream.connect(lowTempStream);
        DataStream<Object> resultStream = connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> value) {
                return new Tuple3<>(value.f0, value.f1, "high temp warning");
            }
            @Override
            public Object map2(SensorReading value) {
                return new Tuple2<>(value.getId(), "normal");
            }
        });

        resultStream.print();

        // union联合多条流
//        warningStream.union(lowTempStream);
        highTempStream.union(lowTempStream,allTempStream);

        environment.execute();

    }
}
