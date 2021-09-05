package top.tzk.flink.stream.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import top.tzk.flink.bean.SensorReading;

public class StateTest1_OperatorState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = environment.readTextFile("src/main/resources/sensor.csv");

        // keyBy
        SingleOutputStreamOperator<SensorReading> dataStream = dataSource
                .map(value -> {
                    String[] fields = value.split(",");
                    return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                });

        // 定义一个有状态的map操作,统计当前分区数据个数
        SingleOutputStreamOperator<Integer> map = dataStream.map(new MyCountMapper());
        map.print();

        environment.execute();
    }

    public static class MyCountMapper implements MapFunction<SensorReading, Integer>, CheckpointedFunction {
        // 定义一个本地变量,作为算子状态
        private Integer count = 0;

        private transient ListState<Integer> globalCountState;

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            return ++count;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            globalCountState.clear();
            globalCountState.add(count);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            globalCountState = context.getOperatorStateStore()
                    .getListState(new ListStateDescriptor<Integer>(
                            "globalCountState",
                            Types.INT));
            if (context.isRestored()){
                for (Integer integer : globalCountState.get()) {
                    count = integer;
                }
            }
        }
    }
}
