package top.tzk.flink.networkflow_analysis.app;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import top.tzk.flink.networkflow_analysis.bean.PageViewCount;
import top.tzk.flink.networkflow_analysis.bean.UserBehavior;

import java.net.URL;
import java.util.Objects;
import java.util.Random;

public class PageViewApp {
    public static void main(String[] args) throws Exception {
        // 1 创建执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(4);

        // 2 读取数据源,创建数据流
        URL resource = PageViewApp.class.getResource("/UserBehavior.csv");
        DataStream<String> dataSource =
                environment.readTextFile(resource.getPath());

        DataStream<UserBehavior> dataStream = dataSource.map(value -> {
            String[] fields = value.split(",");
            return new UserBehavior(Long.valueOf(fields[0]),
                    Long.valueOf(fields[1]),
                    Integer.valueOf(fields[2]),
                    fields[3],
                    Long.valueOf(fields[4]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner((value, timestamp) -> value.getTimestamp() * 1000));

        SingleOutputStreamOperator<PageViewCount> pvStream = dataStream
                .filter(value -> Objects.equals("pv", value.getBehavior()))
                .map(value -> {
                    Random random = new Random();
                    return new Tuple2<>(random.nextInt(16), 1L);
                })
                .returns(Types.TUPLE(Types.INT, Types.LONG))
                .keyBy(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new PvCountAgg(), new PvCountResult());

        pvStream.keyBy(PageViewCount::getWindowEnd)
                .process(new TotalPvCount())
                .print("============>");

        environment.execute();
    }

    private static class PvCountAgg implements AggregateFunction<Tuple2<Integer, Long>, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<Integer, Long> value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    private static class PvCountResult implements WindowFunction<Long, PageViewCount, Integer, TimeWindow> {
        @Override
        public void apply(Integer s, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
            out.collect(new PageViewCount(s.toString(), window.getEnd(), input.iterator().next()));
        }
    }

    private static class TotalPvCount extends KeyedProcessFunction<Long,PageViewCount,PageViewCount> {

        private transient ValueState<Long> countState;

        @Override
        public void processElement(PageViewCount value, Context context, Collector<PageViewCount> collector) throws Exception {
            Long count = countState.value();
            if (Objects.isNull(count)){
                count = 0L;
            }
            countState.update(count + value.getCount());

            context.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<PageViewCount> collector) throws Exception {
            collector.collect(new PageViewCount("pv", context.getCurrentKey(), countState.value()));
            countState.clear();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<>("countState", Types.LONG));
        }
    }
}
