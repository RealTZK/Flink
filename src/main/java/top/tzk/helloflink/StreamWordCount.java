package top.tzk.helloflink;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @Author: tianzhenkun
 * @Date: 2021/1/29
 * @Description:
 * @Modified By:
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 用parameter tool工具从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        // 从Socket文本流读取数据
        DataStream<String> dataStream = environment.socketTextStream(host, port);

        // 基于数据流进行转换计算
        DataStream<Tuple2<String, Integer>> stream = dataStream
                .flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
                    Arrays.stream(value.split(" ")).map(word->new Tuple2(word, 1)).forEach(out::collect);
                })
                // .disableChaining() // 不参与任务链合并
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy((KeySelector<Tuple2<String, Integer>, Object>) value -> value.f0)
                .sum(1).setParallelism(4);

        stream.print();

        // 执行任务
        environment.execute();
    }
}
