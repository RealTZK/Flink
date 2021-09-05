package top.tzk.flink.stream.transform;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class TransformTest1_Base {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = environment.readTextFile("src/main/resources/sensor.csv");

        // 1 map,把string转化为长度输出
        SingleOutputStreamOperator<Integer> map = dataSource.map(String::length);

        // 2 flatMap,按逗号分割字段
        SingleOutputStreamOperator<String> flatMap = dataSource.flatMap((String value, Collector<String> collector) -> {
            Arrays.stream(value.split(",")).forEach(collector::collect);
        }).returns(Types.STRING);

        // 3 filter,筛选温度高于37的数据
        SingleOutputStreamOperator<String> filter = dataSource.filter(value ->
                Double.parseDouble(value.split(",")[2]) > 37);

        map.print("map");
        flatMap.print("flatMap");
        filter.print("filter");
        environment.execute();
    }
}
