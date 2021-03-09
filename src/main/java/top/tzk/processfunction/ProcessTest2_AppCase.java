package top.tzk.processfunction;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import top.tzk.streamAPI.beans.SensorReading;
import top.tzk.util.FlinkUtil;

/**
 * @Author: tianzhenkun
 * @Date: 2021/2/19
 * @Description:
 * @Modified By:
 */
public class ProcessTest2_AppCase {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = FlinkUtil.getStreamExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<String> inputStream = environment.socketTextStream("39.97.123.131", 9090);
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        SingleOutputStreamOperator<String> resultStream = dataStream.keyBy("id")
                .process(new TempConsIncreaseWarning(15));
        resultStream.print();
        FlinkUtil.execute();
    }

    // 实现自定义处理函数,检测一段时间内的温度连续上升,输出报警
    public static class TempConsIncreaseWarning extends KeyedProcessFunction<Tuple, SensorReading, String> {
        private Integer interval;

        public TempConsIncreaseWarning(Integer interval) {
            this.interval = interval;
        }

        // 定义状态,保存上一次的温度值,定时器时间戳
        private ValueState<Double> lastTempState;
        private ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastTempState", Double.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<>("timerTsState", Long.class));
        }

        @Override
        public void processElement(SensorReading sensorReading, Context context, Collector<String> out) throws Exception {
            Double lastTemp = lastTempState.value();
            if (lastTemp == null) {
                lastTempState.update(sensorReading.getTemperature());
                lastTemp = sensorReading.getTemperature();
            }
            Long timerTs = timerTsState.value();
            // 如果温度上升并且没有定时器,注册10秒后的定时器,开始等待
            if (sensorReading.getTemperature() > lastTemp && timerTs == null) {
                long ts = context.timerService().currentProcessingTime() + interval * 1000;
                context.timerService().registerProcessingTimeTimer(ts);
                timerTsState.update(ts);
                timerTs = ts;
            }
            // 如果温度下降,删除定时器
            else if (sensorReading.getTemperature() < lastTemp && timerTs != null){
                context.timerService().deleteProcessingTimeTimer(timerTs);
                timerTsState.clear();
            }
            // 更新温度状态
            lastTempState.update(sensorReading.getTemperature());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect("传感器" + ctx.getCurrentKey().getField(0) + "温度异常上升");
            timerTsState.clear();
        }
    }
}
