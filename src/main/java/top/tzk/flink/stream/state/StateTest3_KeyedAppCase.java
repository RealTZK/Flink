package top.tzk.flink.stream.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import top.tzk.flink.bean.SensorReading;

import java.util.Objects;

public class StateTest3_KeyedAppCase {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = environment.readTextFile("src/main/resources/sensor.csv");

        SingleOutputStreamOperator<Tuple3<String, Double, Double>> dataStream = dataSource
                .map(value -> {
                    String[] fields = value.split(",");
                    return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                })
                .keyBy(SensorReading::getId)
                .flatMap(new TemperatureChangeWarning(2.0));
        dataStream.print();
        environment.execute();
    }

    public static class TemperatureChangeWarning extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {

        private Double threshold;

        public TemperatureChangeWarning(Double threshold) {
            this.threshold = threshold;
        }

        private transient ValueState<Double> temperatureState;

        @Override
        public void open(Configuration parameters) throws Exception {
            temperatureState = getRuntimeContext().getState(new ValueStateDescriptor<>("temperatureState", Types.DOUBLE));
        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> collector) throws Exception {
            // 读取上一次的温度值
            Double lastTemperature = temperatureState.value();

            // 如果不为null,就判断两次温度的差值
            if (Objects.nonNull(lastTemperature)) {
                Double diff = Math.abs(value.getTemperature() - lastTemperature);
                if (diff > threshold) {
                    collector.collect(new Tuple3<>(value.getId(), lastTemperature, value.getTemperature()));
                }
            }

            // 更新状态
            temperatureState.update(value.getTemperature());
        }
    }
}
