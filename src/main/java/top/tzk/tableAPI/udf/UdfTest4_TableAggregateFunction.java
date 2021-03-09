package top.tzk.tableAPI.udf;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import top.tzk.streamAPI.beans.SensorReading;
import top.tzk.util.FlinkUtil;

import java.util.TreeSet;

/**
 * @Author: tianzhenkun
 * @Date: 2021/3/8
 * @Description:
 * @Modified By:
 */
public class UdfTest4_TableAggregateFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = FlinkUtil.getStreamExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = FlinkUtil.getTableEnvironment();
        DataStream<SensorReading> dataStream = FlinkUtil.getSensorReadings(FlinkUtil.getSensorReadingsStrings());
        // 将流转换为表
        Table sensorTable = tableEnvironment.fromDataStream(dataStream, "id, timestamp as ts, temperature");

        // 自定义聚合函数,求当前传感器的top2温度值
        // table api
        TopNTemp topNTemp = new TopNTemp(5);
        tableEnvironment.registerFunction("topNTemp", topNTemp);
        Table resultTable = sensorTable
                .groupBy("id")
                .flatAggregate("topNTemp(temperature) as top_temperature")
                .select("id,top_temperature");

        // sql
        tableEnvironment.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnvironment.sqlQuery("select id, topNTemp(temperature)" +
                " from sensor group by id");

        // 打印输出
        tableEnvironment.toRetractStream(resultTable, Row.class).print("result");
        tableEnvironment.toRetractStream(resultSqlTable, Row.class).print("sql");

        FlinkUtil.execute();
    }

    // 实现自定义AggregateFunction
    public static class TopNTemp extends TableAggregateFunction<Double, TreeSet<Double>> {
        //top n
        private int n;

        public TopNTemp(int n) {
            this.n = n;
        }

        public void accumulate(TreeSet<Double> accumulator, Double temperature) {
            if (accumulator.size() < n) {
                accumulator.add(temperature);
            } else {
                if (accumulator.first() < temperature) {
                    accumulator.remove(accumulator.first());
                    accumulator.add(temperature);
                }
            }
        }

        public void emitValue(TreeSet<Double> accumulator, Collector<Double> collector) {
            for (Double temperature : accumulator) {
                collector.collect(temperature);
            }
        }

        @Override
        public TreeSet<Double> createAccumulator() {
            return new TreeSet<>();
        }
    }
}
