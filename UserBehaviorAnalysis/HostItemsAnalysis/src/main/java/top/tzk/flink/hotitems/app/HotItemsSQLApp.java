package top.tzk.flink.hotitems.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class HotItemsSQLApp {
    public static void main(String[] args) throws Exception {
        // 1 创建执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        environment.setParallelism(1);

        // 2 表的创建
        tableEnvironment.executeSql(
                "CREATE TABLE user_behavior"
                        + "("
                        + "  user_id BIGINT,"
                        + "  item_id BIGINT,"
                        + "  category_id INTEGER,"
                        + "  behavior STRING,"
                        + "  ts BIGINT,"
                        + "  time_ltz AS TO_TIMESTAMP(FROM_UNIXTIME(ts, 'yyyy-MM-dd HH:mm:ss')),"
                        + "  WATERMARK FOR time_ltz AS time_ltz"
                        + ")"
                        + "WITH ("
                        + " 'connector'='filesystem',"
                        + " 'path'='UserBehaviorAnalysis/HostItemsAnalysis/src/main/resources/UserBehavior.csv',"
                        + " 'format'='csv'"
                        + ")"
        );

        Table user_behavior = tableEnvironment.from("user_behavior");
        user_behavior.printSchema();

        // 3 分组开窗
        // table API
        Table windowAggTable = user_behavior
                .filter($("behavior").isEqual("pv"))
                .window(Slide.over(lit(1).hours())
                        .every(lit(5).minutes())
                        .on($("time_ltz"))
                        .as("w"))
                .groupBy($("item_id"), $("w"))
                .select(
                        $("item_id"),
                        $("w").end().as("window_end"),
                        $("item_id").count().as("cnt"));

        // SQL API
        Table windowAggTableSQL = tableEnvironment.sqlQuery("SELECT " +
                "   item_id," +
                "   HOP_END(time_ltz,INTERVAL '5' MINUTES,INTERVAL '1' HOURS) AS window_end," +
                "   COUNT(item_id) AS cnt " +
                "FROM user_behavior " +
                "WHERE behavior = 'pv' " +
                "GROUP BY " +
                "   HOP(time_ltz,INTERVAL '5' MINUTES,INTERVAL '1' HOURS),item_id"
        );

        // 4 利用开窗函数,对count值进行排序并获取Row number,得到topN
        // SQL
        tableEnvironment.createTemporaryView("agg", windowAggTable);
        Table resultTable = tableEnvironment.sqlQuery("SELECT " +
                "* " +
                "FROM " +
                "   (" +
                "   SELECT " +
                "       *,ROW_NUMBER() OVER (PARTITION BY window_end ORDER BY cnt DESC) AS row_num " +
                "   FROM(" +
                "           SELECT " +
                "               item_id," +
                "               HOP_END(time_ltz,INTERVAL '5' MINUTE,INTERVAL '1' HOUR) AS window_end," +
                "               COUNT(item_id) AS cnt " +
                "           FROM user_behavior " +
                "           WHERE " +
                "               behavior = 'pv' " +
                "           GROUP BY " +
                "               HOP(time_ltz,INTERVAL '5' MINUTE,INTERVAL '1' HOUR),item_id" +
                "       )" +
                "   ) " +
                "WHERE" +
                "   row_num <= 5 ");

        tableEnvironment.toRetractStream(resultTable, Row.class).print("-------------------");

        environment.execute();
    }
}
