package top.tzk.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import top.tzk.streamAPI.beans.SensorReading;
import top.tzk.util.FlinkUtil;

/**
 * @Author: tianzhenkun
 * @Date: 2021/2/18
 * @Description:
 * @Modified By:
 */
public class StateTest2_KeyedState {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = FlinkUtil.getStreamExecutionEnvironment();
        environment.setParallelism(1);

        DataStream<String> inputStream = environment.socketTextStream("39.97.123.131", 9090);
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义一个有状态的map操作,统计当前sensor数据个数
        SingleOutputStreamOperator<Integer> map = dataStream
                .keyBy("id")
                .map(new MyKeyCountMapper());

        map.print();

        FlinkUtil.execute();
    }

    public static class MyKeyCountMapper extends RichMapFunction<SensorReading, Integer> {
        private ValueState<Integer> keyCountState;

        // 其他类型状态的声明
        private ListState<String> listState;
        private MapState<String,Double> mapState;
        private ReducingState<SensorReading> reducingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key-count", Integer.class));
            listState = getRuntimeContext().getListState(new ListStateDescriptor<String>("list", String.class));
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>("map", String.class,Double.class));
//            reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<SensorReading>("reduce", new ReduceFunction<SensorReading>(),SensorReading.class);
        }

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            // 其他状态API调用
            // listState
            for (String s : listState.get()) {
                System.out.println("listState->" + s);
            }
            listState.add("hello");
            // mapState
            mapState.get("1");
            mapState.put("2", 11.1);
            // reducingState
            reducingState.add(sensorReading);
            reducingState.clear();

            Integer count = keyCountState.value();
            if (count == null){
                count = 0;
            }
            count ++;
            keyCountState.update(count);
            return count;
        }
    }
}
