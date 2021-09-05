package top.tzk.flink.stream.state;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateTest4_FaultTolerance {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
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
        checkpointConfig.setTolerableCheckpointFailureNumber(2); // 容忍checkpoint失败多少次

        // 重启策略
        // 固定延时重启
        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 10000L));
        // 失败率重启
        environment.setRestartStrategy(RestartStrategies.failureRateRestart(2, Time.minutes(10), Time.minutes(1)));

        environment.execute();
    }
}
