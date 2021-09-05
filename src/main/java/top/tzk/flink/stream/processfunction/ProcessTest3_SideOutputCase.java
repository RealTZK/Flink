package top.tzk.flink.stream.processfunction;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import top.tzk.flink.bean.SensorReading;

public class ProcessTest3_SideOutputCase {
    private final static OutputTag<SensorReading> HIGH = new OutputTag<>("high", Types.POJO(SensorReading.class));
    private final static OutputTag<SensorReading> LOW = new OutputTag<>("low", Types.POJO(SensorReading.class));
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = environment.readTextFile("src/main/resources/sensor.csv");
        // 分流,按照温度值37界分为两条流
        SingleOutputStreamOperator<SensorReading> sensorReadings = dataSource.process(new ProcessFunction<>() {
            @Override
            public void processElement(String value, Context context, Collector<SensorReading> collector) throws Exception {
                String[] fields = value.split(",");
                SensorReading sensorReading = new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                if (sensorReading.getTemperature() > 37) {
                    context.output(HIGH, sensorReading);
                } else {
                    context.output(LOW, sensorReading);
                }
                collector.collect(sensorReading);
            }
        });

        // 高温流
        DataStream<SensorReading> high = sensorReadings.getSideOutput(HIGH);
        high.print("high");
        // 低温流
        DataStream<SensorReading> low = sensorReadings.getSideOutput(LOW);
        low.print("low");
        environment.execute();
    }
}
