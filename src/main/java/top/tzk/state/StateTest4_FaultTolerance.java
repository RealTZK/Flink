package top.tzk.state;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import top.tzk.streamAPI.beans.SensorReading;
import top.tzk.util.FlinkUtil;

import java.io.IOException;

/**
 * @Author: tianzhenkun
 * @Date: 2021/2/19
 * @Description:
 * @Modified By:
 */
public class StateTest4_FaultTolerance {
    public static void main(String[] args) throws IOException {
        StreamExecutionEnvironment environment = FlinkUtil.getStreamExecutionEnvironment();
        environment.setParallelism(1);

        // 状态后端配置
        environment.setStateBackend(new MemoryStateBackend());
        environment.setStateBackend(new FsStateBackend(""));
        environment.setStateBackend(new RocksDBStateBackend(""));

        // 检查点配置
        environment.enableCheckpointing(10000L);

        // 高级选项
        CheckpointConfig checkpointConfig = environment.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setCheckpointTimeout(10000L);
        checkpointConfig.setMaxConcurrentCheckpoints(2); // 同时进行的checkpoint
        checkpointConfig.setMinPauseBetweenCheckpoints(100L); // 上个checkpoint完成与下个开始的最小时间间隔
        checkpointConfig.setPreferCheckpointForRecovery(true); // 倾向于检查点恢复
        checkpointConfig.setTolerableCheckpointFailureNumber(2); // 容忍checkpoint失败多少次

        // 重启策略
        // 固定延时重启
        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 10000L));
        // 失败率重启
        environment.setRestartStrategy(RestartStrategies.failureRateRestart(2, Time.minutes(10), Time.minutes(1)));

        DataStream<String> inputStream = environment.socketTextStream("39.97.123.131", 9090);
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        dataStream.print();
        FlinkUtil.execute();
    }
}
