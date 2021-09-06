package top.tzk.flink.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class TableTest2_CommonApi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
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

        // 聚合计算
        Table aggAPITable = inputTable.groupBy($("id"))
                .select($("id"),
                        $("id").count().as("cnt"),
                        $("temperature").avg().as("avgTemp"));

        // SQL
        Table resultSQLTable = tableEnvironment.sqlQuery("SELECT id,temperature FROM inputTable WHERE id = '1009'");
        Table aggSQLTable = tableEnvironment.sqlQuery("SELECT id,COUNT(id) AS cnt,AVG(temperature) AS avgTemp FROM inputTable GROUP BY id");

        tableEnvironment.toAppendStream(resultAPITable, Row.class).print("resultAPITable");
        tableEnvironment.toRetractStream(aggAPITable, Row.class).print("aggAPITable");
        tableEnvironment.toAppendStream(resultSQLTable, Row.class).print("resultSQLTable");
        tableEnvironment.toRetractStream(aggSQLTable, Row.class).print("aggSQLTable");

        environment.execute();
    }
}
