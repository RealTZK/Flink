package top.tzk.tableAPI.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;
import top.tzk.streamAPI.beans.SensorReading;
import top.tzk.util.FlinkUtil;

/**
 * @Author: tianzhenkun
 * @Date: 2021/3/8
 * @Description:
 * @Modified By:
 */
public class UdfTest3_AggregateFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = FlinkUtil.getStreamExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = FlinkUtil.getTableEnvironment();
        DataStream<SensorReading> dataStream = FlinkUtil.getSensorReadings(FlinkUtil.getSensorReadingsStrings());
        // 将流转换为表
        Table sensorTable = tableEnvironment.fromDataStream(dataStream, "id, timestamp as ts, temperature");

        // 自定义聚合函数,求当前传感器的平均温度值
        // table api
        AvgTemp avgTemp = new AvgTemp();
        tableEnvironment.registerFunction("avgTemp",avgTemp);
        Table resultTable = sensorTable
                .groupBy("id")
                .aggregate("avgTemp(temperature)  as avgtemp")
                .select("id, avgtemp");

        // sql
        tableEnvironment.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnvironment.sqlQuery("select id, avgTemp(temperature)" +
                " from sensor group by id");

        // 打印输出
        tableEnvironment.toRetractStream(resultTable, Row.class).print("result");
        tableEnvironment.toRetractStream(resultSqlTable, Row.class).print("sql");

        FlinkUtil.execute();
    }

    // 实现自定义AggregateFunction
    public static class AvgTemp extends AggregateFunction<Double,Tuple2<Double,Integer>> {
        @Override
        public Double getValue(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }

        // 必须实现一个accumulate方法,来数据之后更新状态
        public void accumulate(Tuple2<Double,Integer> accumulator, Double temp){
            accumulator.f0 += temp;
            accumulator.f1 ++ ;
        }

    }
}
