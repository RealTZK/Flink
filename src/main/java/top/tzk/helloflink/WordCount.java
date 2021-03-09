package top.tzk.helloflink;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.io.FileInputStream;

/**
 * @Author: tianzhenkun
 * @Date: 2021/1/29
 * @Description: 批处理word count
 * @Modified By:
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取数据
        String inputPath = "src/main/resources/hello.txt";
        int read = new FileInputStream(inputPath).read();
        System.out.println(read);
        DataSet<String> dataSource = environment.readTextFile(inputPath);

        // 对数据集进行处理,按空格分词展开,转换成(word,1)进行二元组进行统计
        DataSet<Tuple2<String, Integer>> resultSet = dataSource
                .flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
                    String[] words = value.split(" ");
                    for (String word : words) {
                        out.collect(new Tuple2<>(word, 1));
                    }
                })
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .groupBy(0) // 按照第一个位置的word分组
                .sum(1) // 将第二个位置上的数据求和
                .sortPartition(1, Order.ASCENDING).setParallelism(1);

        resultSet.print();
    }
}
