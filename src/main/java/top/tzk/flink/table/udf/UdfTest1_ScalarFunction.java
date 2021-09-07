package top.tzk.flink.table.udf;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class UdfTest1_ScalarFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        // 创建表的执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        // 表的创建
        tableEnvironment.executeSql(
                "CREATE TABLE sensor"
                        + "("
                        + "  id STRING,"
                        + "  ts BIGINT,"
                        + "  temperature DOUBLE,"
                        + "  time_ltz AS TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000, 'yyyy-MM-dd HH:mm:ss')),"
                        + "  WATERMARK FOR time_ltz AS time_ltz - INTERVAL '5' SECOND"
                        + ")"
                        + "WITH ("
                        + " 'connector'='filesystem',"
                        + " 'path'='src/main/resources/sensor.csv',"
                        + " 'format'='csv'"
                        + ")"
        );

        // 自定义标量函数，实现求id的hash值
        ScalarFunction hashCode = new HashCode(13);
        // 在环境中注册UDF
        tableEnvironment.createTemporaryFunction("hashcode", hashCode);
        // table API
        Table sensor = tableEnvironment.from("sensor");
        Table resultAPITable = sensor.select($("id"), $("ts"), call("hashcode", $("id")));
        Table resultSQLTable = tableEnvironment.sqlQuery("SELECT id,hashcode(id) FROM sensor");

        tableEnvironment.toAppendStream(resultAPITable, Row.class).print("result");
        tableEnvironment.toAppendStream(resultSQLTable, Row.class).print("sql");

        environment.execute();
    }

    // 实现自定义标量函数ScalarFunction
    public static class HashCode extends ScalarFunction{
        private int factor = 13;
        public HashCode(int factor){
            this.factor = factor;
        }
        public int eval(String value){
            return value.hashCode() * factor;
        }
    }
}
