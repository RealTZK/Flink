package top.tzk.flink.stream.window;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import top.tzk.flink.bean.SensorReading;

public class WindowTest1_TimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = environment.readTextFile("src/main/resources/sensor.csv");
        DataStream<SensorReading> dataStream = dataSource.map(value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });

        // 增量聚合函数
        SingleOutputStreamOperator<Integer> resultStream = dataStream.keyBy(SensorReading::getId)
                // .countWindow(10, 2) // 计数窗口
                // .window(EventTimeSessionWindows.withGap(Time.minutes(1))) // 会话窗口
                // .window(SlidingProcessingTimeWindows.of(Time.hours(1), Time.minutes(5))) // 滑动窗口
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10))) // 滚动窗口
                // .reduce((value1, value2) -> null)
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
        resultStream.print("增量聚合");

        // 全窗口函数
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> apply = dataStream.keyBy(SensorReading::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                /*.process(new ProcessWindowFunction<>() {
                    @Override
                    public void process(String key, Context context, Iterable<SensorReading> elements, Collector<Tuple3<String, Long, Integer>> collector) {

                    }
                })*/
                .apply(new WindowFunction<>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<SensorReading> input, Collector<Tuple3<String, Long, Integer>> collector) throws Exception {
                        long windowEnd = window.getEnd();
                        int size = IteratorUtils.toList(input.iterator()).size();
                        collector.collect(new Tuple3<>(key, windowEnd, size));
                    }
                });

        // 其他可选API
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late"){};
        SingleOutputStreamOperator<SensorReading> sum = dataStream.keyBy(SensorReading::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                // .trigger()
                // .evictor()
                .allowedLateness(Time.minutes(1))
                .sum("temperature");
        sum.getSideOutput(outputTag).print("late");

        resultStream.print("增量聚合");
        apply.print("全聚合");
        sum.print("sum");
        environment.execute();
    }
}
