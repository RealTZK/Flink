package top.tzk.flink.stream.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import top.tzk.flink.bean.SensorReading;

import java.util.Objects;

public class StateTest2_KeyedState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = environment.readTextFile("src/main/resources/sensor.csv");

        // keyBy
        SingleOutputStreamOperator<Integer> dataStream = dataSource
                .map(value -> {
                    String[] fields = value.split(",");
                    return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                })
                .keyBy(SensorReading::getId)
                .map(new MyCountMapper());
        dataStream.print();
        environment.execute();
    }

    public static class MyCountMapper extends RichMapFunction<SensorReading, Integer> {
        private transient ValueState<Integer> keyCountState;

        // 其他类型状态的声明
        private transient ListState<String> listState;
        private transient MapState<String, Double> mapState;
        private transient ReducingState<SensorReading> reducingState;

        @Override
        public Integer map(SensorReading value) throws Exception {
            Integer count = keyCountState.value();
            if (Objects.isNull(count)) {
                count = 0;
            }
            count++;
            keyCountState.update(count);
            return count;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<>("keyCountState", Types.INT));
            listState = getRuntimeContext().getListState(new ListStateDescriptor<>("listState", Types.STRING));
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("mapState", Types.STRING, Types.DOUBLE));
            /*reducingState = getIterationRuntimeContext().getReducingState(
                    new ReducingStateDescriptor<>("reducingState", new ReduceFunction<>() {
                        @Override
                        public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                            return null;
                        }
                    }, Types.POJO(SensorReading.class)));*/
        }
    }
}
