package top.tzk.flink.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class TableTest3_FileOutput {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        // 创建表的执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        // 表的创建
        tableEnvironment.executeSql(
                "CREATE TABLE inputTable"
                        + "("
                        + "  id STRING,"
                        + "  ts BIGINT,"
                        + "  temperature DOUBLE"
                        + ")"
                        + "WITH ("
                        + " 'connector'='filesystem',"
                        + " 'path'='src/main/resources/sensor.csv',"
                        + " 'format'='csv'"
                        + ")"
        );

        Table inputTable = tableEnvironment.from("inputTable");
        inputTable.printSchema();
        tableEnvironment.toAppendStream(inputTable, Row.class).print("inputTable");

        // table API
        // 简单查询
        Table resultAPITable = inputTable.select($("id"), $("temperature"))
                .filter($("id").isEqual("1009"));
        Table aggAPITable = inputTable.groupBy($("id"))
                .select($("id"),
                        $("id").count().as("cnt"),
                        $("temperature").avg().as("avgTemp"));

        // 表的创建
        tableEnvironment.executeSql(
                "CREATE TABLE outputResultTable"
                        + "("
                        + "  id STRING,"
                        + "  temperature DOUBLE"
                        + ")"
                        + "WITH ("
                        + " 'connector'='filesystem',"
                        + " 'path'='src/main/resources/result.csv',"
                        + " 'format'='csv'"
                        + ")"
        );

        tableEnvironment.executeSql(
                "CREATE TABLE outputAggTable"
                        + "("
                        + "  id STRING,"
                        + "  cnt BIGINT,"
                        + "  temperature DOUBLE"
                        + ")"
                        + "WITH ("
                        + " 'connector'='filesystem',"
                        + " 'path'='src/main/resources/agg.csv',"
                        + " 'format'='csv'"
                        + ")"
        );

        resultAPITable.executeInsert("outputResultTable");
        // aggAPITable.executeInsert("outputAggTable"); // 该connector不支持更新操作

        environment.execute();
    }
}
