package top.tzk.flink.table;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import top.tzk.flink.bean.SensorReading;

import static org.apache.flink.table.api.Expressions.$;

public class Example {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = environment.readTextFile("src/main/resources/sensor.csv");

        SingleOutputStreamOperator<SensorReading> dataStream = dataSource
                .map(value -> {
                    String[] fields = value.split(",");
                    return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                });

        // 创建表的执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        // 基于流创建表
        Table table = tableEnvironment.fromDataStream(dataStream);

        // 调用tableAPI
        Table resultAPITable = table.select($("id"), $("ts"), $("temperature"))
                .where($("id").isEqual("1001"));

        // 执行sql语句
        tableEnvironment.createTemporaryView("sensor", table);
        String sql = "SELECT id,ts,temperature FROM sensor WHERE id = '1001' ";
        Table resultSQLTable = tableEnvironment.sqlQuery(sql);

        // print
        tableEnvironment.toAppendStream(resultAPITable, Row.class).print("API");
        tableEnvironment.toAppendStream(resultSQLTable, Row.class).print("SQL");

        environment.execute();
    }
}
