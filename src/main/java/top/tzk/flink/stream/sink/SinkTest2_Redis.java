package top.tzk.flink.stream.sink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import top.tzk.flink.bean.SensorReading;
import top.tzk.flink.stream.source.MessageGenerator;

public class SinkTest2_Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<SensorReading> dataStream = environment.addSource(new MessageGenerator());
        FlinkJedisConfigBase configBase = new FlinkJedisPoolConfig.Builder()
                .setHost("host")
                .setPort(0000)
                .setPassword("password")
                .build();
        dataStream.addSink(new RedisSink<>(configBase, new RedisMapper<>() {
            // 定义保存到redis的命令，保存成hash表，hset sensor_temperature id temperature
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET,"sensor_temperature");
            }

            @Override
            public String getKeyFromData(SensorReading value) {
                return value.getId();
            }

            @Override
            public String getValueFromData(SensorReading value) {
                return value.getTemperature().toString();
            }
        }));

        environment.execute();
    }
}
