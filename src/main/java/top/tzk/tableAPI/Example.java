package top.tzk.tableAPI;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import top.tzk.streamAPI.beans.SensorReading;
import top.tzk.util.FlinkUtil;

/**
 * @Author: tianzhenkun
 * @Date: 2021/2/23
 * @Description:
 * @Modified By:
 */
public class Example {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = FlinkUtil.getStreamExecutionEnvironment();
        environment.setParallelism(1);

        // 1.读取数据
        DataStream<String> inputStream = environment.readTextFile("flink/src/main/resources/sensor.txt");

        // 2.转换成POJO
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 3.创建表的执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        // 4.基于流创建表
        Table dataTable = tableEnvironment.fromDataStream(dataStream);

        // 5.调用tableAPI进行转换操作
        Table resultTable = dataTable.select("id,temperature")
                .where("id = '1001'");

        // 6.执行sql语句
        tableEnvironment.createTemporaryView("sensor", dataTable);
        String sql = "select id,temperature from sensor where id = '1001' ";
        Table resultSqlTable = tableEnvironment.sqlQuery(sql);

        // 7.table的sink
        tableEnvironment.toAppendStream(resultTable, Row.class).print("result");
        tableEnvironment.toAppendStream(resultSqlTable, Row.class).print("resultSql");

        FlinkUtil.execute();

    }
}
