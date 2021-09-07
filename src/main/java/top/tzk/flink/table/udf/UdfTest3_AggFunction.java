package top.tzk.flink.table.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class UdfTest3_AggFunction {
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

        // 自定义聚合函数,求传感器温度均值
        AggregateFunction<Double, Tuple2<Double, Integer>> avgTemp = new AvgTemp();
        // 在环境中注册UDF
        tableEnvironment.createTemporaryFunction("avgTemp", avgTemp);
        // table API
        Table sensor = tableEnvironment.from("sensor");
        Table resultAPITable = sensor
                .groupBy($("id"))
                .aggregate(call("avgTemp", $("temperature")).as("avgtemp"))
                .select($("id"), $("avgtemp"));
        // SQL
        Table resultSQLTable = tableEnvironment.sqlQuery("SELECT id,avgTemp(temperature) " +
                "FROM sensor GROUP BY id");

        tableEnvironment.toRetractStream(resultAPITable, Row.class).print("result");
        tableEnvironment.toRetractStream(resultSQLTable, Row.class).print("sql");

        environment.execute();
    }

    // 实现自定义聚合函数AggregateFunction
    public static class AvgTemp extends AggregateFunction<Double, Tuple2<Double, Integer>> {
        @Override
        public Double getValue(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0D, 0);
        }

        // 必须实现一个accumulate方法,来数据之后更新状态
        public void accumulate(Tuple2<Double, Integer> accumulator, Double value) {
            accumulator.f0 += value;
            accumulator.f1++;
        }
    }
}
