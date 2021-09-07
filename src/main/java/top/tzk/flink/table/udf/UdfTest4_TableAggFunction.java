package top.tzk.flink.table.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class UdfTest4_TableAggFunction {
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

        // 自定义聚合函数,求当前传感器温度topN温度值
        TopNTemp top3Temp = new TopNTemp();
        // 在环境中注册UDF
        tableEnvironment.createTemporaryFunction("topNTemp", top3Temp);
        // table API
        Table sensor = tableEnvironment.from("sensor");
        Table resultAPITable = sensor
                .groupBy($("id"))
                .flatAggregate(call("topNTemp", $("temperature")).as("top_temperature"))
                .select($("id"), $("top_temperature"));
        // SQL
        Table resultSQLTable = tableEnvironment.sqlQuery("SELECT id,topNTemp(temperature) " +
                "FROM sensor GROUP BY id");

        tableEnvironment.toRetractStream(resultAPITable, Row.class).print("result");
        tableEnvironment.toRetractStream(resultSQLTable, Row.class).print("sql");

        environment.execute();
    }

    // 实现自定义聚合函数TableAggregateFunction
    public static class TopNTemp extends TableAggregateFunction<Double, Tuple2<Double, Double>> {
        public void accumulate(Tuple2<Double, Double> accumulator, Double value) {
            if (value > accumulator.f0) {
                accumulator.f1 = accumulator.f0;
                accumulator.f0 = value;
            } else if (value > accumulator.f1) {
                accumulator.f1 = value;
            }
        }

        public void emitValue(Tuple2<Double, Double> accumulator, Collector<Double> collector) {
            collector.collect(accumulator.f0);
            collector.collect(accumulator.f1);
        }

        @Override
        public Tuple2<Double, Double> createAccumulator() {
            return new Tuple2<>(0D, 0D);
        }
    }
}
