package top.tzk.windowAPI;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import top.tzk.streamAPI.beans.SensorReading;
import top.tzk.util.FlinkUtil;

/**
 * @Author: tianzhenkun
 * @Date: 2021/2/5
 * @Description:
 * @Modified By:
 */
public class WindowTest1_TimeWindow {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = FlinkUtil.getStreamExecutionEnvironment();
        DataStream<String> stringSource = environment.socketTextStream("39.97.123.131", 9090);
        DataStream<SensorReading> sensorReadings = FlinkUtil.getSensorReadings(stringSource);

        // 增量聚合函数
        DataStream<Integer> resultStream = sensorReadings.keyBy("id")
//                .countWindow(10,2);
//                .window(EventTimeSessionWindows.withGap(Time.minutes(1)));
                .timeWindow(Time.seconds(10))
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)));

//        .reduce((ReduceFunction<SensorReading>) (sensorReading, t1) -> null)
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {

                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReading sensorReading, Integer integer) {
                        return integer + 1;
                    }

                    @Override
                    public Integer getResult(Integer integer) {
                        return integer;
                    }

                    @Override
                    public Integer merge(Integer integer, Integer acc1) {
                        return integer + acc1;
                    }
                });

        // 全窗口函数
        SingleOutputStreamOperator<Tuple3<String,Long,Integer>> apply = sensorReadings.keyBy("id")
                .timeWindow(Time.seconds(10))
//                .process(new ProcessWindowFunction<SensorReading, context, Tuple, TimeWindow>() {
//                })
                .apply(new WindowFunction<SensorReading, Tuple3<String,Long,Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<SensorReading> iterable, Collector<Tuple3<String,Long,Integer>> collector) throws Exception {
                        String id = tuple.getField(0);
                        Long windowEnd = timeWindow.getEnd();
                        int size = IteratorUtils.toList(iterable.iterator()).size();
                        collector.collect(new Tuple3<>(id, windowEnd, size));
                    }
                });

        // 其他可选API
        OutputTag<SensorReading> outputTag = new OutputTag<>("late");
        SingleOutputStreamOperator<SensorReading> sumStream = sensorReadings.keyBy("id")
                .timeWindow(Time.seconds(10))
//                .trigger()
//                .evictor()
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .sum("temperature");

        sumStream.getSideOutput(outputTag).print("late");

        resultStream.print("增量聚合");

        apply.print("全聚合");

        sumStream.print("等待数据");
        FlinkUtil.execute();
    }
}
