package top.tzk.windowAPI;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import top.tzk.streamAPI.beans.SensorReading;
import top.tzk.util.FlinkUtil;

/**
 * @Author: tianzhenkun
 * @Date: 2021/2/18
 * @Description:
 * @Modified By:
 */
public class WindowTest3_EventTimeWindow {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = FlinkUtil.getStreamExecutionEnvironment();
//        environment.setParallelism(1);
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> inputStream = environment.socketTextStream("39.97.123.131", 9090);
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        })
                // 升序数据设置事件时间和watermark
//                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
//                    @Override
//                    public long extractAscendingTimestamp(SensorReading sensorReading) {
//                        return sensorReading.getTimestamp() ;
//                    }
//                });
                // 乱序数据设置事件时间和watermark
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(SensorReading sensorReading) {
                        return sensorReading.getTimestamp();
                    }
                });


        OutputTag<SensorReading> late = new OutputTag<>("late");
        // 基于事件时间的开窗聚合
        SingleOutputStreamOperator<SensorReading> minTempStream = dataStream.keyBy("id")
                .timeWindow(Time.milliseconds(5000))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(late)
                .maxBy("temperature");

        minTempStream.print("minTemp");
        minTempStream.getSideOutput(late).print("late");

        FlinkUtil.execute();
    }
}
