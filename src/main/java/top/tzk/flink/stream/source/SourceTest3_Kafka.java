package top.tzk.flink.stream.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class SourceTest3_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "host:port");
        properties.setProperty("group.id", "groupId");
        DataStreamSource<String> kafkaSource = environment.addSource(new FlinkKafkaConsumer<>(
                "sensor",
                new SimpleStringSchema(),
                properties
        ));
        environment.execute();
    }
}
