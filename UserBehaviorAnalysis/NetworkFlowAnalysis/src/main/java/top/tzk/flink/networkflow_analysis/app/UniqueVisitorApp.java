package top.tzk.flink.networkflow_analysis.app;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import top.tzk.flink.networkflow_analysis.bean.PageViewCount;
import top.tzk.flink.networkflow_analysis.bean.UserBehavior;

import java.net.URL;
import java.util.Objects;

public class UniqueVisitorApp {
    public static void main(String[] args) throws Exception {
        // 1 创建执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        // 2 读取数据源,创建数据流
        URL resource = UniqueVisitorApp.class.getResource("/UserBehavior.csv");
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

        SingleOutputStreamOperator<PageViewCount> uvStream = dataStream
                .filter(value -> Objects.equals("pv", value.getBehavior()))
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .trigger(new MyTrigger())
                .process(new UvCountResult());

        uvStream.print("==================>");

        environment.execute();
    }

    private static class MyTrigger extends Trigger<UserBehavior, TimeWindow> {
        @Override
        public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext context) throws Exception {
            // 每一条数据来到,直接触发窗口计算,并且直接清空窗口
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext context) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext context) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext context) throws Exception {
        }
    }

    // 自定义一个布隆过滤器
    public static class MyBloomFilter {
        // 定义位图的大小,一般需要定义为2的整数次幂
        private final int cap;
        private final int seed;
        private final int[] bitMap;
        private int size;

        public MyBloomFilter(int cap, int seed) {
            this.cap = (cap - (cap & 1)) * 2;
            this.seed = seed;
            bitMap = new int[this.cap];
            size = 0;
        }

        public boolean exists(String key) {
            int hashcode = hashcode(key, seed);
            int i = bitMap[hashcode];
            if (i == 0) {
                bitMap[hashcode] = 1;
                size++;
                return false;
            } else {
                return true;
            }
        }

        public int getSize() {
            return size;
        }

        // 实现一个hash函数
        private int hashcode(String value, int seed) {
            int result = 0;
            for (int i = 0; i < value.length(); i++) {
                result = result * seed + value.charAt(i);
            }
            return result & (cap - 1);
        }
    }

    private static class UvCountResult extends ProcessAllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {

        // 声明布隆过滤器
        private MyBloomFilter filter;

        @Override
        public void process(Context context, Iterable<UserBehavior> elements, Collector<PageViewCount> out) throws Exception {
            UserBehavior userBehavior = elements.iterator().next();
            if (165681L == userBehavior.getUserId()){
                System.out.println("111");
            }
            filter.exists(userBehavior.getUserId().toString());
            int size = filter.size;
            PageViewCount result = new PageViewCount("uv", context.window().getEnd(), (long) size);
            out.collect(result);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            filter = new MyBloomFilter(1 << 28, 3421);
        }
    }
}
