package top.tzk.flink.hello;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        String inputPath = "src/main/resources/hello.txt";
        DataStream<String> dataSource = environment.readTextFile(inputPath);
        DataStream<Tuple2<String, Integer>> resultSet = dataSource
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (line, collector) -> {
                    String[] words = line.split(" ");
                    Arrays.stream(words).map(word -> new Tuple2<>(word,1)).forEach(collector::collect);
                })
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(value -> value.f0)
                .sum(1);
        resultSet.print();
        environment.execute();
    }
}
