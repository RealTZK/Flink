package top.tzk.processfunction;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import top.tzk.streamAPI.beans.SensorReading;
import top.tzk.util.FlinkUtil;

/**
 * @Author: tianzhenkun
 * @Date: 2021/2/20
 * @Description:
 * @Modified By:
 */
public class ProcessTest3_SideOutputCase {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = FlinkUtil.getStreamExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<String> inputStream = environment.socketTextStream("39.97.123.131", 9090);
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义OutputTag,用来表示侧输出流,低温流
        OutputTag<SensorReading> lowTemp = new OutputTag<SensorReading>("lowTemp"){};

        // 测试processFunction,自定义侧输出流实现分流操作
        SingleOutputStreamOperator<SensorReading> highTempStream = dataStream
                .process(new ProcessFunction<SensorReading, SensorReading>() {
                    @Override
                    public void processElement(SensorReading sensorReading, Context context, Collector<SensorReading> out) throws Exception {
                        // 判断温度
                        if (sensorReading.getTemperature() > 37) {
                            out.collect(sensorReading);
                        }else {
                            context.output(lowTemp, sensorReading);
                        }
                    }
                });

        highTempStream.print("high");
        highTempStream.getSideOutput(lowTemp).print("low");

        FlinkUtil.execute();
    }
}
