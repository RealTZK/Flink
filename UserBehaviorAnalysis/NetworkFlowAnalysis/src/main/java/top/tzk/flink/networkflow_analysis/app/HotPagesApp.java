package top.tzk.flink.networkflow_analysis.app;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import top.tzk.flink.networkflow_analysis.bean.ApacheLogEvent;
import top.tzk.flink.networkflow_analysis.bean.PageViewCount;

import java.net.URL;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

public class HotPagesApp {
    public static void main(String[] args) throws Exception {
        // 1 创建执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        // 2 读取数据源,创建数据流
        URL resource = HotPagesApp.class.getResource("/apache.log");
        DataStream<String> dataSource =
                environment.readTextFile(resource.getPath());

        DataStream<ApacheLogEvent> dataStream = dataSource.map(value -> {
            String[] fields = value.split(" ");
            SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            Long timestamp = dateFormat.parse(fields[3]).getTime();
            return new ApacheLogEvent(fields[0], fields[1], timestamp, fields[5], fields[6]);
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<ApacheLogEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((value, timestamp) -> value.getTimestamp()));

        OutputTag<ApacheLogEvent> late = new OutputTag<>("late") {
        };

        // 分组开窗聚合
        SingleOutputStreamOperator<PageViewCount> windowAggStream = dataStream
                .filter(value -> {
                    String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                    return Pattern.matches(regex, value.getUrl());
                })
                .filter(value -> Objects.equals("GET", value.getMethod()))
                .keyBy(ApacheLogEvent::getUrl)
                .window(SlidingEventTimeWindows.of(Time.minutes(30), Time.minutes(1)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(late)
                .aggregate(new PageCountAgg(), new PageCountResult());

        windowAggStream.getSideOutput(late).print("late----->");

        // 收集统一窗口count数据,排序输出
        SingleOutputStreamOperator<String> resultStream = windowAggStream.keyBy(PageViewCount::getWindowEnd)
                .process(new TopNHotPages(3));

        resultStream.print("==========");

        environment.execute();
    }

    private static class PageCountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent value, Long accumulator) {
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

    private static class PageCountResult implements WindowFunction<Long, PageViewCount, String, TimeWindow> {
        @Override
        public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
            out.collect(new PageViewCount(s, window.getEnd(), input.iterator().next()));
        }
    }

    private static class TopNHotPages extends KeyedProcessFunction<Long, PageViewCount, String> {
        private final int N;

        public TopNHotPages(int n) {
            N = n;
        }

        // 声明状态,保存当前所有PageViewCount到List中
        MapState<String, Long> state;

        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<String> out) throws Exception {
            state.put(value.getUrl(), value.getCount());
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + Time.minutes(1).toMilliseconds());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 先判断是否到了窗口关闭清理时间,如果是,直接清空状态并返回
            if (timestamp == ctx.getCurrentKey() + Time.minutes(60).toMilliseconds()){
                state.clear();
                return;
            }

            ArrayList<Map.Entry<String, Long>> pageViewCounts = Lists.newArrayList(state.entries());
            pageViewCounts.sort((var1, var2) -> Long.compare(var2.getValue(), var1.getValue()));

            StringBuilder result = new StringBuilder();
            result.append("窗口结束时间:").append(new Timestamp(timestamp - 1)).append("\n");
            for (int i = 0; i < Math.min(N, pageViewCounts.size()); i++) {
                Map.Entry<String, Long> value = pageViewCounts.get(i);
                result.append("NO ").append(i + 1).append(":")
                        .append(" 页面URL = ").append(value.getKey())
                        .append(" 浏览量 = ").append(value.getValue())
                        .append("\n");
            }
            result.append("====================================\n\n");

            // 控制输出频率
            Thread.sleep(Time.milliseconds(500).toMilliseconds());
            out.collect(result.toString());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getMapState(new MapStateDescriptor<>(
                    "pageCountList", Types.STRING, Types.LONG));
        }
    }
}
