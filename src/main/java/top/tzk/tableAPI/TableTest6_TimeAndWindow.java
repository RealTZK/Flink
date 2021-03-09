package top.tzk.tableAPI;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import top.tzk.streamAPI.beans.SensorReading;
import top.tzk.util.FlinkUtil;

/**
 * @Author: tianzhenkun
 * @Date: 2021/2/25
 * @Description:
 * @Modified By:
 */
public class TableTest6_TimeAndWindow {
    public static void main(String[] args) {

        StreamTableEnvironment tableEnvironment = FlinkUtil.getTableEnvironment();
        FlinkUtil.setParallelism(1);
        FlinkUtil.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> inputStream = FlinkUtil.getSensorReadingsStrings();

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)){
                    @Override
                    public long extractTimestamp(SensorReading sensorReading) {
                        return sensorReading.getTimestamp();
                    }
                });

//        Table dataTable = tableEnvironment.fromDataStream(dataStream, "id,timestamp,temperature,pt.proctime");
//        Table dataTable = tableEnvironment.fromDataStream(dataStream, "id,timestamp.rowtime,temperature");
        Table dataTable = tableEnvironment.fromDataStream(dataStream, "id,timestamp,temperature,rt.rowtime");
        tableEnvironment.createTemporaryView("sensor", dataTable);

//        tableEnvironment.connect(new Kafka()
//                .version("0.11")
//                .topic("sensor")
//                .property("zookeeper.connect", "39.97.123.131:2181")
//                .property("bootstrap.servers", "39.97.123.131:9092")
//        )
//                .withFormat(new Csv())
//                .withSchema(new Schema()
//                        .field("id", DataTypes.STRING())
//                        .field("timestamp", DataTypes.BIGINT())
//                        .rowtime(new Rowtime()
//                                .timestampsFromField("timestamp")
//                                .watermarksPeriodicBounded(1000)
//                        )
//                        .field("temperature", DataTypes.DOUBLE())
//                )
//                .createTemporaryTable("inputTable");

        // 窗口操作
        // Group Window
        // table API
        Table resultTable = dataTable.window(Tumble.over("10.seconds").on("rt").as("tw"))
                .groupBy("id,tw")
                .select("id,id.count,temperature.avg,tw.end");

        // SQL
        Table sqlTable = tableEnvironment.sqlQuery("select id,count(id),avg(temperature),tumble_end(rt,interval '10' second)" +
                "from sensor group by id,tumble(rt,interval '10' second)");

//        dataTable.printSchema();

//        tableEnvironment.toAppendStream(dataTable, Row.class).print();

        // Over Window
        // Table API
        Table overResult = dataTable.window(Over.partitionBy("id").orderBy("rt").preceding("2.rows").as("ow"))
                .select("id,rt,id.count over ow,temperature.avg over ow");

        // SQL
        Table overSqlResult = tableEnvironment.sqlQuery("select id, rt, count(id) over ow, avg(temperature) over ow " +
                " from sensor " +
                " window ow as (partition by id order by rt rows between 2 preceding and current row) ");

//        tableEnvironment.toAppendStream(resultTable, Row.class).print("result");
//        tableEnvironment.toRetractStream(sqlTable, Row.class).print("sql");
        tableEnvironment.toAppendStream(overResult, Row.class).print("result");
        tableEnvironment.toRetractStream(overSqlResult, Row.class).print("sql");
        FlinkUtil.execute();
    }
}
