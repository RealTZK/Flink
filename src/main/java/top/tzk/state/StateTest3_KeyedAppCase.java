package top.tzk.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import top.tzk.streamAPI.beans.SensorReading;
import top.tzk.util.FlinkUtil;

/**
 * @Author: tianzhenkun
 * @Date: 2021/2/18
 * @Description:
 * @Modified By:
 */
public class StateTest3_KeyedAppCase {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = FlinkUtil.getStreamExecutionEnvironment();
        environment.setParallelism(1);

        DataStream<String> inputStream = environment.socketTextStream("39.97.123.131", 9090);
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义一个flatMap操作,检测温度跳变,输出报警
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> resultStream = dataStream.keyBy("id")
                .flatMap(new TimeChangeWarning(10.0));

        resultStream.print();

        FlinkUtil.execute();
    }

    public static class TimeChangeWarning extends RichFlatMapFunction<SensorReading, Tuple3<String,Double,Double>>{
        private Double threshold;
        public TimeChangeWarning(Double threshold) {
            this.threshold = threshold;
        }
        // 定义状态,保存上一个温度值
        private ValueState<Double> lastTempState;

        @Override
        public void open(Configuration parameters) {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemp", Double.class));
        }

        @Override
        public void flatMap(SensorReading sensorReading, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            // 获取上一次的温度值
            Double lastTemp = lastTempState.value();

            // 如果不为null,就判断两次温度插值
            if (lastTemp != null){
                Double diff = Math.abs(sensorReading.getTemperature() - lastTemp);
                if (diff >= threshold){
                    out.collect(new Tuple3<>(sensorReading.getId(),lastTemp,sensorReading.getTemperature()));
                }
            }

            // 更新状态
            lastTempState.update(sensorReading.getTemperature());
        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
        }
    }
}
