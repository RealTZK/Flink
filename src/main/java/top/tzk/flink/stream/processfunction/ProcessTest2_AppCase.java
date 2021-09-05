package top.tzk.flink.stream.processfunction;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import top.tzk.flink.bean.SensorReading;

import java.util.Objects;

public class ProcessTest2_AppCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = environment.readTextFile("src/main/resources/sensor.csv");

        // keyBy
        SingleOutputStreamOperator<String> result = dataSource
                .map(value -> {
                    String[] fields = value.split(",");
                    return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                })
                .keyBy(SensorReading::getId)
                .process(new TemperatureIncreaseWarning(Time.milliseconds(1)));

        result.print();
        environment.execute();
    }

    // 实现自定义处理函数,检测一段时间内的温度连续上升,输出报警
    public static class TemperatureIncreaseWarning extends KeyedProcessFunction<String, SensorReading, String> {
        private Time interval;

        public TemperatureIncreaseWarning(Time interval) {
            this.interval = interval;
        }

        // 定义状态,保存上一次的温度值,定时器时间戳
        private ValueState<Double> temperatureState;
        private ValueState<Long> timerState;

        @Override
        public void processElement(SensorReading value, Context context, Collector<String> collector) throws Exception {
            Double lastTemperature = temperatureState.value();
            if (Objects.isNull(lastTemperature)) {
                temperatureState.update(value.getTemperature());
                lastTemperature = value.getTemperature();
            }
            Long timer = timerState.value();
            // 如果温度上升并且没有定时器,注册定时器,开始等待
            if (value.getTemperature() > lastTemperature && Objects.isNull(timer)) {
                long ts = context.timerService().currentProcessingTime() + interval.toMilliseconds();
                context.timerService().registerProcessingTimeTimer(ts);
                timerState.update(ts);
                timer = ts;
            }
            // 如果温度下降,删除定时器
            else if (value.getTemperature() < lastTemperature && Objects.nonNull(timer)){
                context.timerService().deleteProcessingTimeTimer(timer);
                timerState.clear();
            }
            // 更新温度状态
            temperatureState.update(value.getTemperature());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<String> collector) throws Exception {

        }

        @Override
        public void open(Configuration parameters) throws Exception {
            temperatureState = getRuntimeContext().getState(new ValueStateDescriptor<Double>(
                    "temperatureState",
                    Types.DOUBLE));
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>(
                    "timerState",
                    Types.LONG));
        }
    }
}
