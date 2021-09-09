package top.tzk.flink.hotitems.app;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class HotItemsApp {
    public static void main(String[] args) throws Exception{
        // 1 创建执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        // 2 读取数据源,创建DataStream数据流
        DataStream<String> dataSource = environment.readTextFile("src/main/resources/UserBehavior.csv");
        dataSource.print();
        environment.execute();
    }
}
