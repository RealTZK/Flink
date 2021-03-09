package top.tzk.streamAPI.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import top.tzk.streamAPI.beans.SensorReading;

/**
 * @Author: tianzhenkun
 * @Date: 2021/2/5
 * @Description:
 * @Modified By:
 */
public class SinkTest1_Kafka {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStream<String> streamSource = environment.readTextFile("flink/src/main/resources/sensor.txt");

        DataStream<String> sensorReadings = streamSource.map(s -> {
            String[] split = s.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2])).toString();
        });

        sensorReadings.addSink(new FlinkKafkaProducer011<String>("39.97.123.131:9092","sensor",new SimpleStringSchema()));

        environment.execute();
    }
}
