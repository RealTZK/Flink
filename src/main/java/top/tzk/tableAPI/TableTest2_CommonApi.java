package top.tzk.tableAPI;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import top.tzk.util.FlinkUtil;

/**
 * @Author: tianzhenkun
 * @Date: 2021/2/23
 * @Description:
 * @Modified By:
 */
public class TableTest2_CommonApi {
    public static void main(String[] args) {

        StreamExecutionEnvironment environment = FlinkUtil.getStreamExecutionEnvironment();
        environment.setParallelism(1);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        // 1.1 基于老版本planner的流处理
        EnvironmentSettings oldStreamSettings = EnvironmentSettings.newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment oldStreamTableEnvironment = StreamTableEnvironment.create(environment, oldStreamSettings);

        // 1.2 基于老版本planner的批处理
        ExecutionEnvironment batchEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment oldBatchTableEnvironment = BatchTableEnvironment.create(batchEnvironment);

        // 2.1 基于Blink的流处理
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnvironment = StreamTableEnvironment.create(environment, blinkStreamSettings);

        // 2.2 基于Blink的批处理
        EnvironmentSettings blinkBatchSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();
        TableEnvironment blinkBatchTableEnvironment = TableEnvironment.create(blinkBatchSettings);

        // 3.表的创建:连接外部系统,读取数据
        // 3.1 读取文件
        tableEnvironment.connect(new FileSystem().path("src/main/resources/sensor.txt"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable");

        Table inputTable = tableEnvironment.from("inputTable");
//        inputTable.printSchema();
//        tableEnvironment.toAppendStream(inputTable, Row.class).print();

        // 4.查询转换
        // 4.1 table API
        // 简单转换
        Table resultTable = inputTable.select("id,temp")
                .filter("id = '1009'");

        // 聚合计算
        Table aggTable = inputTable.groupBy("id")
                .select("id,id.count as count,temp.avg as avgTemp");

        // 4.2 SQL
        Table table = tableEnvironment.sqlQuery("select id,temp from inputTable where id = '1009'");
        Table sqlAggTable = tableEnvironment.sqlQuery("select id,count(id) as cnt,avg(temp) as avgTemp from inputTable group by id");

        // 打印输出
        tableEnvironment.toAppendStream(resultTable, Row.class).print("result");
        tableEnvironment.toRetractStream(aggTable, Row.class).print("aggTable");
        tableEnvironment.toAppendStream(table, Row.class).print("table");
        tableEnvironment.toRetractStream(sqlAggTable, Row.class).print("sqlAggTable");

        FlinkUtil.execute();
    }
}
