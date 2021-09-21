package top.tzk.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class AddressTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 创建表的执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        // 表的创建
        tableEnvironment.executeSql(
                "CREATE TABLE inputTable"
                        + "("
                        + "  user_id BIGINT,"
                        + "  item_id BIGINT,"
                        + "  category_id INTEGER,"
                        + "  behavior STRING,"
                        + "  ts BIGINT,"
                        + "  time_ltz AS TO_TIMESTAMP(FROM_UNIXTIME(ts, 'yyyy-MM-dd HH:mm:ss'))"
                        + ")"
                        + "WITH ("
                        + " 'connector'='filesystem',"
                        + " 'path'='UserBehaviorAnalysis/HostItemsAnalysis/src/main/resources/UserBehavior.csv',"
                        + " 'format'='csv'"
                        + ")"
        );

        Table inputTable = tableEnvironment.from("inputTable");
        inputTable.printSchema();
        tableEnvironment.toAppendStream(inputTable, Row.class).print("inputTable");

        environment.execute();
    }
}
