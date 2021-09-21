package top.tzk.flink.hotitems.app;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import top.tzk.flink.hotitems.beans.ItemViewCount;
import top.tzk.flink.hotitems.beans.UserBehavior;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Objects;

public class HotItemsApp {
    public static void main(String[] args) throws Exception {
        // 1 创建执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        // 2 读取数据源,创建DataStream数据流
        DataStream<String> dataSource =
                environment.readTextFile("UserBehaviorAnalysis/HostItemsAnalysis/src/main/resources/UserBehavior.csv");

        // 3 转换为POJO，分配时间戳和watermark
        DataStream<UserBehavior> dataStream = dataSource.map(value -> {
            String[] fields = value.split(",");
            return new UserBehavior(
                    Long.valueOf(fields[0]),
                    Long.valueOf(fields[1]),
                    Integer.valueOf(fields[2]),
                    fields[3],
                    Long.valueOf(fields[4]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner((value, timestamp) -> value.getTimestamp() * 1000));

        // 4 分组开窗聚合，得到每个窗口内各个商品的count值
        DataStream<ItemViewCount> windowAggStream = dataStream
                .filter(value -> Objects.equals("pv", value.getBehavior()))
                .keyBy(UserBehavior::getItemId)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new ItemCountAgg(), new WindowItemCountResult());

        // 5 收集同一窗口的所有数据,排序输出topN
        DataStream<String> resultStream = windowAggStream
                .keyBy(ItemViewCount::getWindowEnd) // 按照窗口分组
                .process(new TopNHotItemOperator(5)); // 用自定义处理函数排序取前N

        resultStream.print("result=========>>");

        environment.execute("HotItemsApp");
    }

    // 实现自定义增量聚合函数
    public static class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long accumulator1, Long accumulator2) {
            return accumulator1 + accumulator2;
        }
    }

    // 实现自定义全窗口函数
    public static class WindowItemCountResult implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {
        @Override
        public void apply(Long key, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> collector) {
            Long value = input.iterator().next();
            collector.collect(new ItemViewCount(key, window.getEnd(), value));
        }
    }

    // 实现自定义KeyedProcessFunction
    public static class TopNHotItemOperator extends KeyedProcessFunction<Long, ItemViewCount, String> {
        private int N;

        public TopNHotItemOperator(int n) {
            N = n;
        }

        // 声明列表状态,保存当前窗口内所有输出各个Item的Count值
        private transient ListState<Tuple2<Long, Long>> itemCountState;

        @Override
        public void processElement(ItemViewCount value, Context context, Collector<String> collector) throws Exception {
            // 每来一条数据,存入状态中,并注册定时器
            itemCountState.add(new Tuple2<>(value.getItemId(), value.getCount()));
            context.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<String> collector) throws Exception {
            // 排序输出topN
            ArrayList<Tuple2<Long, Long>> itemCounts = Lists.newArrayList(itemCountState.get().iterator());
            itemCounts.sort((value1, value2) -> Long.compare(value2.f1, value1.f1));

            // 将排序信息序列化为String
            StringBuilder result = new StringBuilder();
            result.append("窗口结束时间:").append(new Timestamp(timestamp - 1)).append("\n");
            for (int i = 0; i < Math.min(N, itemCounts.size()); i++) {
                Tuple2<Long, Long> value = itemCounts.get(i);
                result.append("NO ").append(i + 1).append(":")
                        .append(" 商品ID = ").append(value.f0)
                        .append(" 热门度 = ").append(value.f1)
                        .append("\n");
            }
            result.append("====================================\n\n");

            // 控制输出频率
            Thread.sleep(Time.seconds(1).toMilliseconds());
            collector.collect(result.toString());
        }

        @Override
        public void open(Configuration parameters) {
            itemCountState = getRuntimeContext().getListState(new ListStateDescriptor<>(
                    "itemCountState",
                    Types.TUPLE(Types.LONG, Types.LONG)));
        }
    }
}
