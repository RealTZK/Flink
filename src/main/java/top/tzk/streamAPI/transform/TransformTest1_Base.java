package top.tzk.streamAPI.transform;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @Author: tianzhenkun
 * @Date: 2021/2/4
 * @Description:
 * @Modified By:
 */
public class TransformTest1_Base {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStreamSource<String> streamSource = environment.readTextFile("flink/src/main/resources/sensor.txt");

        // 1.map,把string转换为长度输出
        SingleOutputStreamOperator<Integer> map = streamSource.map(String::length);

        // 2.flatMap,按逗号切分字段
        SingleOutputStreamOperator<String> flatMap = streamSource.flatMap((String value, Collector<String> out) -> {
            Arrays.stream(value.split(",")).forEach(out::collect);
        }).returns(Types.STRING);

        // 3.filter,筛选温度高于37的数据
        SingleOutputStreamOperator<String> filter = streamSource.filter(s -> Double.parseDouble(s.split(",")[2]) > 37);

        // 打印输出
        map.print("map");
        flatMap.print("flatMap");
        filter.print("filter");

        environment.execute();

    }
}
