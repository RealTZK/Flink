package top.tzk.processfunction;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
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
public class ProcessTest1_KeyedProcessFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = FlinkUtil.getStreamExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<String> inputStream = environment.socketTextStream("39.97.123.131", 9090);
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 测试KeyedProcessFunction
        SingleOutputStreamOperator<Integer> resultStream = dataStream.keyBy("id")
                .process(new KeyedProcessFunction<Tuple, SensorReading, Integer>() {
                    ValueState<Long> tsTimer;

                    @Override
                    public void open(Configuration parameters) {
                        tsTimer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("tsTimer", Long.class));
                    }

                    @Override
                    public void processElement(SensorReading sensorReading, Context context, Collector<Integer> out) throws Exception {
                        out.collect(sensorReading.getId().length());

                        // context
                        context.timestamp();
                        context.getCurrentKey();
//                        context.output();
                        TimerService timerService = context.timerService();
                        timerService.currentProcessingTime();
                        timerService.currentWatermark();
                        timerService.registerProcessingTimeTimer(timerService.currentProcessingTime() + 1000L);
                        tsTimer.update(timerService.currentProcessingTime() + 1000L);
                        timerService.registerEventTimeTimer(sensorReading.getTimestamp() + 10);
//                        timerService.deleteProcessingTimeTimer(tsTimer.value());
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext context, Collector<Integer> out) throws Exception {
                        System.out.println(timestamp + "定时器触发");
                        context.getCurrentKey();
//                        context.output();
                        context.timeDomain();
                    }

                    @Override
                    public void close() {
                        tsTimer.clear();
                    }
                });

        resultStream.print();

        FlinkUtil.execute();
    }
}
