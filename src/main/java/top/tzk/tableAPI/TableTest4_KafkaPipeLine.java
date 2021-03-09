package top.tzk.tableAPI;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import top.tzk.util.FlinkUtil;

/**
 * @Author: tianzhenkun
 * @Date: 2021/2/24
 * @Description:
 * @Modified By:
 */
public class TableTest4_KafkaPipeLine {
    public static void main(String[] args) {
        TableEnvironment tableEnvironment = FlinkUtil.getTableEnvironment();
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
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable");

        // 查询转换
        Table sensorTable = tableEnvironment.from("inputTable");
        Table resultTable = sensorTable.select("id,temp")
                .filter("id = '1009'");

        // 聚合计算
        Table aggTable = sensorTable.groupBy("id")
                .select("id,id.count as count,temp.avg as avgTemp");

        // sink
        tableEnvironment.connect(new Kafka()
                .version("0.11")
                .topic("sinkSensor")
                .property("zookeeper.connect", "39.97.123.131:2181")
                .property("bootstrap.servers", "39.97.123.131:9092")
        )
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temp", DataTypes.DOUBLE())
                )
                .createTemporaryTable("outputTable");

        resultTable.insertInto("outputTable");

        FlinkUtil.execute();
    }
}
