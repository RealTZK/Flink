package top.tzk.tableAPI.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import top.tzk.streamAPI.beans.SensorReading;
import top.tzk.util.FlinkUtil;

/**
 * @Author: tianzhenkun
 * @Date: 2021/3/8
 * @Description:
 * @Modified By:
 */
public class UdfTest2_TableFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = FlinkUtil.getStreamExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = FlinkUtil.getTableEnvironment();
        DataStream<SensorReading> dataStream = FlinkUtil.getSensorReadings(FlinkUtil.getSensorReadingsStrings());
        // 将流转换为表
        Table sensorTable = tableEnvironment.fromDataStream(dataStream, "id, timestamp as ts, temperature");

        // 自定义表函数,实现对id的拆分,并输出
        // table api
        Split split = new Split(2);
        tableEnvironment.registerFunction("split",split);
        Table resultTable = sensorTable
                .joinLateral("split(id) as (locationNo,areaNo)")
                .select("id, ts, locationNo, areaNo");

        // sql
        tableEnvironment.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnvironment.sqlQuery("select id, ts, locationNo, areaNo" +
                " from sensor, lateral table(split(id)) as address(locationNo, areaNo)");

        // 打印输出
        tableEnvironment.toAppendStream(resultTable, Row.class).print("result");
        tableEnvironment.toAppendStream(resultSqlTable, Row.class).print("sql");

        FlinkUtil.execute();
    }

    // 实现自定义tableFunction
    public static class Split extends TableFunction<Tuple2<String, String>> {

        // 定义分割位
        private int splitLocation;

        public Split(int splitLocation) {
            this.splitLocation = splitLocation;
        }

        // 必须实现的eval方法,无返回值
        public void eval(String string) {
            String locationNo = string.substring(splitLocation);
            String areaNo = string.substring(0, splitLocation);
            this.collector.collect(new Tuple2<>(areaNo, locationNo));
        }
    }
}
