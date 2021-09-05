package top.tzk.flink.stream.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import top.tzk.flink.bean.SensorReading;
import top.tzk.flink.stream.source.MessageGenerator;

public class SinkTest1_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = environment.addSource(new MessageGenerator())
                .map(SensorReading::toString);
        dataStream.addSink(new FlinkKafkaProducer<String>(
                "host:port",
                "sensor",
                new SimpleStringSchema()));
        environment.execute();
    }
}
