package top.tzk.tableAPI.udf;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import top.tzk.streamAPI.beans.SensorReading;
import top.tzk.util.FlinkUtil;

/**
 * @Author: tianzhenkun
 * @Date: 2021/3/8
 * @Description:
 * @Modified By:
 */
public class UdfTest1_ScalarFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = FlinkUtil.getStreamExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = FlinkUtil.getTableEnvironment();
        DataStream<SensorReading> dataStream = FlinkUtil.getSensorReadings(FlinkUtil.getSensorReadingsStrings());

        // 将流转换为表
        Table sensorTable = tableEnvironment.fromDataStream(dataStream,"id, timestamp as ts, temperature");

        // 自定义标量函数,实现求id的hash值
        // 1.table API
        HashCode hashCode = new HashCode(23);
        // 需要在环境中注册UDF
        tableEnvironment.registerFunction("hashcode",hashCode);
        Table resultTable = sensorTable.select("id,ts,hashcode(id)");

        // 2.SQL
        tableEnvironment.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnvironment.sqlQuery("select id, ts, hashcode(id) from sensor");

        // 打印输出
        tableEnvironment.toAppendStream(resultTable, Row.class).print("result");
        tableEnvironment.toAppendStream(resultSqlTable, Row.class).print("sql");

        FlinkUtil.execute();

    }

    // 实现自定义标量函数ScalarFunction
    public static class HashCode extends ScalarFunction{
        private int factor = 13;

        public HashCode(int factor) {
            this.factor = factor;
        }

        public int eval(String string){
            return string.hashCode() * factor;
        }
    }
}
