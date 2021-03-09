package top.tzk.tableAPI;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import top.tzk.util.FlinkUtil;

/**
 * @Author: tianzhenkun
 * @Date: 2021/2/24
 * @Description:
 * @Modified By:
 */
public class TableTest3_FileOutput {
    public static void main(String[] args) {
        TableEnvironment tableEnvironment = FlinkUtil.getTableEnvironment();
        FlinkUtil.setParallelism(1);

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

        // 5. 输出到文件
        // 链接外部文件注册输出表
        tableEnvironment.connect(new FileSystem().path("src/main/resources/result.txt"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temperature", DataTypes.DOUBLE())
                )
                .createTemporaryTable("outputResultTable");

        tableEnvironment.connect(new FileSystem().path("src/main/resources/agg.txt"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("cnt", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE())
                )
                .createTemporaryTable("outputAggTable");

        resultTable.insertInto("outputResultTable");
//        aggTable.insertInto("outputAggTable"); // 不支持更新操作

        FlinkUtil.execute();
    }
}
