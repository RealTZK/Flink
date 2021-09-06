package top.tzk.flink.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

public class TableTest6_TimeAndWindow {
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
                        + "  temperature DOUBLE,"
                        + "  time_ltz AS TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000, 'yyyy-MM-dd HH:mm:ss')),"
                        + "  WATERMARK FOR time_ltz AS time_ltz - INTERVAL '5' SECOND"
                        + ")"
                        + "WITH ("
                        + " 'connector'='filesystem',"
                        + " 'path'='src/main/resources/sensor.csv',"
                        + " 'format'='csv'"
                        + ")"
        );

        Table inputTable = tableEnvironment.from("inputTable");
        inputTable.printSchema();

        // 窗口操作

        // Group window
        // table API
        Table resultAPITable = inputTable.window(Tumble.over(lit(5).seconds()).on($("time_ltz")).as("tw"))
                .groupBy($("id"), $("tw"))
                .select($("id"), $("id").count(), $("temperature").avg(), $("tw").end());
        tableEnvironment.toAppendStream(inputTable, Row.class).print("inputTable");
        // SQL
        Table resultSQLTable = tableEnvironment.sqlQuery("SELECT id,COUNT(id),AVG(temperature),TUMBLE_END(time_ltz,INTERVAL '5' SECOND) " +
                "FROM inputTable GROUP BY id,TUMBLE(time_ltz,INTERVAL '5' SECOND)");

        //Over window
        // table API
        Table overAPIResult = inputTable
                .window(Over
                        .partitionBy($("id"))
                        .orderBy($("time_ltz"))
                        .preceding(rowInterval(2L))
                        .as("ow"))
                .select($("id"),
                        $("time_ltz"),
                        $("id").count().over($("ow")),
                        $("temperature").avg().over($("ow")));
        // SQL
        Table overSQLResult = tableEnvironment.sqlQuery("SELECT id,time_ltz,COUNT(id) OVER ow,AVG(temperature) OVER ow " +
                "FROM inputTable " +
                "WINDOW ow AS (PARTITION BY id ORDER BY time_ltz rows BETWEEN 2 PRECEDING AND CURRENT row)");

        tableEnvironment.toAppendStream(resultAPITable, Row.class).print("resultAPITable");
        tableEnvironment.toRetractStream(resultSQLTable, Row.class).print("resultSQLTable");
        tableEnvironment.toAppendStream(overAPIResult, Row.class).print("overAPIResult");
        tableEnvironment.toRetractStream(overSQLResult, Row.class).print("overSQLResult");

        environment.execute();
    }
}
