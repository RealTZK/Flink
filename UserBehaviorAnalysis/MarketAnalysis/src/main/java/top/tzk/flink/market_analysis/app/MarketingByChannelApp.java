package top.tzk.flink.market_analysis.app;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import top.tzk.flink.market_analysis.bean.MarketingUserBehavior;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;

public class MarketingByChannelApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        // 1 从自定义数据源中读取数据
        DataStream<MarketingUserBehavior> dataStream = environment.addSource(new SimulatedSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<MarketingUserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((value, timestamp) -> value.getTimestamp()));

        // 2 分渠道开窗统计
        dataStream.filter(value -> !Objects.equals("UNINSTALL", value.getBehavior()))
                .keyBy(value -> new Tuple2<>(value.getChannel(), value.getBehavior()), Types.TUPLE(Types.STRING, Types.STRING))
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(5)));

        environment.execute();
    }

    private static class SimulatedSource implements SourceFunction<MarketingUserBehavior> {

        // 控制是否正常运行的标志位
        boolean running = true;

        // 声明用户行为和渠道的范围
        List<String> behaviors = Arrays.asList("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL");
        List<String> channels = Arrays.asList("APP_STORE", "WECHAT", "QQ", "WEB");

        Random random = new Random();

        @Override
        public void run(SourceContext<MarketingUserBehavior> context) throws Exception {
            while (running) {
                Long id = random.nextLong();
                String behavior = behaviors.get(random.nextInt(behaviors.size()));
                String channel = channels.get(random.nextInt(channels.size()));
                long timestamp = Math.round(random.nextGaussian() * 1000) + System.currentTimeMillis();

                context.collect(new MarketingUserBehavior(id, behavior, channel, timestamp));

                Thread.sleep(200);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

    }
}
