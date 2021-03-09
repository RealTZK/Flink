package top.tzk.tableAPI;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;
import top.tzk.util.FlinkUtil;

/**
 * @Author: tianzhenkun
 * @Date: 2021/2/25
 * @Description:
 * @Modified By:
 */
public class TableTest5_EsSink {
    public static void main(String[] args) {
        StreamTableEnvironment tableEnvironment = FlinkUtil.getTableEnvironment();
        FlinkUtil.setParallelism(1);

        // 连接kafka读取数据
        tableEnvironment.connect(new Kafka()
                .version("0.11")
                .topic("sensor")
                .property("zookeeper.connect", "39.97.123.131:2181")
                .property("bootstrap.servers", "39.97.123.131:9092")
        )
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE())
                )
                .inAppendMode()
                .createTemporaryTable("inputTable");

//        tableEnvironment.connect(new FileSystem().path("src/main/resources/sensor.txt"))
//                .withFormat(new Csv())
//                .withSchema(new Schema()
//                        .field("id", DataTypes.STRING())
//                        .field("ts", DataTypes.BIGINT())
//                        .field("temp", DataTypes.DOUBLE())
//                )
//                .createTemporaryTable("inputTable");

        // 查询转换
        Table sensorTable = tableEnvironment.from("inputTable");

        sensorTable.printSchema();

        // 聚合计算
        Table aggTable = sensorTable.groupBy("id")
                .select("id,id.count as count,temp.avg as avgTemp");

        aggTable.printSchema();

        tableEnvironment.toRetractStream(aggTable, Row.class).print();

        // sink
        tableEnvironment.connect(new Elasticsearch()
                .version("7")
                .host("39.97.123.131",9200,"http")
                .index("sensortemp")
                .documentType("temp")
        )
                .inUpsertMode()
                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("count", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE())
                )
                .createTemporaryTable("outputTable");

        aggTable.insertInto("outputTable");

        FlinkUtil.execute();
    }
}