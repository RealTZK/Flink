package top.tzk.flink.stream.window;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import top.tzk.flink.bean.SensorReading;

import java.time.Duration;

public class WindowTest2_EventTimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(9);
        DataStreamSource<String> dataSource = environment.readTextFile("src/main/resources/sensor.csv");
        DataStream<SensorReading> dataStream = dataSource.map(value -> {
                    String[] fields = value.split(",");
                    return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((value, timestamp) -> value.getTimestamp())
                );

        OutputTag<SensorReading> late= new OutputTag<>("late"){};

        // 增量聚合函数
        SingleOutputStreamOperator<SensorReading> resultStream = dataStream.keyBy(SensorReading::getId)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(5000)))
                .allowedLateness(Time.milliseconds(500))
                .sideOutputLateData(late)
                .maxBy("temperature");

        resultStream.print("result").setParallelism(1);
        resultStream.getSideOutput(late).print("late");
        environment.execute();
    }
}
