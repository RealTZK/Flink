package top.tzk.flink.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FunctionTest_Time {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        // 创建表的执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        // 表的创建
        tableEnvironment.executeSql(""
                + "CREATE TABLE inputTable ("
                + " id STRING,"
                + " ts BIGINT,"
                + " temperature DOUBLE "
                + ") WITH ("
                + " 'connector'='filesystem',"
                + " 'path'='src/main/resources/sensor.csv',"
                + " 'format'='csv'"
                + ")"
        );

        tableEnvironment.executeSql("" +
                "CREATE TABLE print_table " +
                "WITH (" +
                "   'connector' = 'print'," +
                "   'print-identifier' = '========================'" +
                ")" +
                "LIKE inputTable (EXCLUDING ALL)"
        );

        tableEnvironment.executeSql("" +
                "INSERT INTO print_table " +
                "SELECT " +
                "* " +
                "FROM " +
                "   inputTable"
        );

        environment.execute();
    }
}
