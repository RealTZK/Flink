package top.tzk.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import top.tzk.streamAPI.beans.SensorReading;
import top.tzk.util.FlinkUtil;

import java.util.Collections;
import java.util.List;

/**
 * @Author: tianzhenkun
 * @Date: 2021/2/18
 * @Description:
 * @Modified By:
 */
public class StateTest1_OperatorState {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = FlinkUtil.getStreamExecutionEnvironment();
        environment.setParallelism(1);

        DataStream<String> inputStream = environment.socketTextStream("39.97.123.131", 9090);
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义一个有状态的map操作,统计当前分区数据个数
        SingleOutputStreamOperator<Integer> map = dataStream.map(new MyCountMapper());

        map.print();

        FlinkUtil.execute();
    }

    public static class MyCountMapper implements MapFunction<SensorReading, Integer>, ListCheckpointed<Integer>{
        // 定义一个本地变量,作为算子状态
        private Integer count = 0;
        @Override
        public Integer map(SensorReading sensorReading) {
            return ++count;
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) {
            return Collections.singletonList(count);
        }

        @Override
        public void restoreState(List<Integer> list) {
            for (Integer num : list) {
                count += num;
            }
        }
    }
}
