package top.tzk.flink.hello;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Arrays;

/**
 * 批处理WordCount
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String inputPath = "src/main/resources/hello.txt";
        DataSet<String> dataSource = environment.readTextFile(inputPath);
        DataSet<Tuple2<String, Integer>> resultSet = dataSource
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (line, collector) -> {
                    String[] words = line.split(" ");
                    Arrays.stream(words).map(word -> new Tuple2<>(word, 1)).forEach(collector::collect);
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .groupBy(0)
                .sum(1)
                .sortPartition(1, Order.DESCENDING);
        resultSet.print();
    }
}
