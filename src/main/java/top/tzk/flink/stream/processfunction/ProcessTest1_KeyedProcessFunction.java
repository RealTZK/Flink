package top.tzk.flink.stream.processfunction;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import top.tzk.flink.bean.SensorReading;

public class ProcessTest1_KeyedProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = environment.readTextFile("src/main/resources/sensor.csv");

        // keyBy
        SingleOutputStreamOperator<Integer> result = dataSource
                .map(value -> {
                    String[] fields = value.split(",");
                    return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                })
                .keyBy(SensorReading::getId)
                .process(new KeyedProcessFunction<>() {
                    private transient ValueState<Long> timerState;

                    @Override
                    public void processElement(SensorReading value, Context context, Collector<Integer> collector) throws Exception {
                        collector.collect(value.getId().length());

                        // context
                        context.timestamp();
                        context.getCurrentKey();
//                        context.output();
                        TimerService timerService = context.timerService();
                        timerService.currentProcessingTime();
                        timerService.currentWatermark();
                        timerService.registerProcessingTimeTimer(timerService.currentProcessingTime() + 1000L);
                        timerState.update(timerService.currentProcessingTime() + 1000L);
                        timerService.registerEventTimeTimer(value.getTs() + 10);
//                        timerService.deleteProcessingTimeTimer(tsTimer.value());
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext context, Collector<Integer> out) throws Exception {
                        System.out.println(timestamp + "定时器触发," + context.getCurrentKey() + "," + context.timeDomain());
                    }

                    @Override
                    public void open(Configuration parameters) {
                        timerState = getRuntimeContext().getState(new ValueStateDescriptor<>(
                                "timerState",
                                Types.LONG));
                    }

                    @Override
                    public void close() throws Exception {
                        timerState.clear();
                    }
                });
        environment.execute();
    }
}
