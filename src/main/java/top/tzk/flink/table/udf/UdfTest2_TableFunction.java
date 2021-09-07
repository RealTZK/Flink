package top.tzk.flink.table.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class UdfTest2_TableFunction {
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

        // 自定义表函数，实现对id的拆分，并输出
        TableFunction<Tuple2<String, String>> split = new Split(2);
        // 在环境中注册UDF
        tableEnvironment.createTemporaryFunction("split", split);
        // table API
        Table sensor = tableEnvironment.from("sensor");
        Table resultAPITable = sensor
                .joinLateral(call("split", $("id")).as("locationNo", "areaNo"))
                .select($("id"), $("ts"), $("locationNo"), $("areaNo"));
        // SQL
        Table resultSQLTable = tableEnvironment.sqlQuery("SELECT id,ts,locationNo,areaNo FROM sensor," +
                "LATERAL TABLE(split(id)) AS ADDRESS(locationNo,areaNo)");

        tableEnvironment.toAppendStream(resultAPITable, Row.class).print("result");
        tableEnvironment.toAppendStream(resultSQLTable, Row.class).print("sql");

        environment.execute();
    }

    // 实现自定义标量函数ScalarFunction
    public static class Split extends TableFunction<Tuple2<String, String>> {
        private int position = 13;

        public Split(int position) {
            this.position = position;
        }

        // 必须实现的eval方法,无返回值
        public void eval(String value) {
            String locationNo = value.substring(position);
            String areaNo = value.substring(0, position);
            collect(new Tuple2<>(areaNo, locationNo));
        }
    }
}
