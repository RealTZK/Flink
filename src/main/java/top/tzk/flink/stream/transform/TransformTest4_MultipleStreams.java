package top.tzk.flink.stream.transform;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import top.tzk.flink.bean.SensorReading;

public class TransformTest4_MultipleStreams {
    private final static OutputTag<SensorReading> HIGH = new OutputTag<>("high", TypeInformation.of(SensorReading.class));
    private final static OutputTag<SensorReading> LOW = new OutputTag<>("low", TypeInformation.of(SensorReading.class));

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = environment.readTextFile("src/main/resources/sensor.csv");

        // 分流,按照温度值37界分为两条流
        SingleOutputStreamOperator<SensorReading> sensorReadings = dataSource.process(new ProcessFunction<>() {
            @Override
            public void processElement(String value, Context context, Collector<SensorReading> collector) throws Exception {
                String[] fields = value.split(",");
                SensorReading sensorReading = new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                if (sensorReading.getTemperature() > 37) {
                    context.output(HIGH, sensorReading);
                } else {
                    context.output(LOW, sensorReading);
                }
                collector.collect(sensorReading);
            }
        });

        // 高温流
        DataStream<SensorReading> high = sensorReadings.getSideOutput(HIGH);
        high.print("high");
        // 低温流
        DataStream<SensorReading> low = sensorReadings.getSideOutput(LOW);
        low.print("low");

        // 合流,将高温流转化为Tuple2类型,与低温流连接合并之后,输出状态信息
        DataStream<Tuple2<String, Double>> warningStream = high
                .map(value -> new Tuple2<>(value.getId(), value.getTemperature()))
                .returns(Types.TUPLE(Types.STRING,Types.DOUBLE));
        ConnectedStreams<Tuple2<String, Double>, SensorReading> connect = warningStream.connect(low);
        SingleOutputStreamOperator<Object> resultStream = connect.map(new CoMapFunction<>() {
            @Override
            public Object map1(Tuple2<String, Double> value) {
                return new Tuple3<>(value.f0, value.f1, "high temperature warning!");
            }

            @Override
            public Object map2(SensorReading value) {
                return new Tuple2<>(value.getId(), "normal");
            }
        });

        resultStream.print("result");

        // union联合多条流
        high.union(low,sensorReadings);

        environment.execute();
    }
}
