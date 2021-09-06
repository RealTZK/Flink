package top.tzk.flink.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class TableTest5_Elasticsearch {
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
                        + " 'connector'='kafka',"
                        + " 'topic'='sensor',"
                        + " 'properties.bootstrap.servers'='host:port',"
                        + " 'properties.group.id'='groupId',"
                        + " 'scan.startup.mode'='earliest-offset',"
                        + " 'format'='json'"
                        + ")"
        );

        Table inputTable = tableEnvironment.from("inputTable");
        inputTable.printSchema();
        tableEnvironment.toAppendStream(inputTable, Row.class).print("inputTable");

        // table API
        // 简单查询
        Table resultAPITable = inputTable.select($("id"), $("temperature"))
                .filter($("id").isEqual("1009"));

        // 表的创建
        tableEnvironment.executeSql(
                "CREATE TABLE outputResultTable"
                        + "("
                        + "  id STRING,"
                        + "  temperature DOUBLE"
                        + "  PRIMARY KEY (id) NOT ENFORCED"
                        + ")"
                        + "WITH ("
                        + " 'connector'='elasticsearch-7',"
                        + " 'hosts'='host:port',"
                        + " 'index'='sensor'"
                        + ")"
        );

        resultAPITable.executeInsert("outputResultTable");

        environment.execute();
    }
}
