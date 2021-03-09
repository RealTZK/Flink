package top.tzk.streamAPI.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import top.tzk.streamAPI.beans.SensorReading;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Random;

/**
 * @Author: tianzhenkun
 * @Date: 2021/2/5
 * @Description:
 * @Modified By:
 */
public class SinkTest4_JDBC {

    public static final String JDBC_URL =
            "jdbc:mysql://realtzk.top:3306/flink?useUnicode=true&characterEncoding=UTF-8&socketTimeout=15000&autoReconnectForPools=true&allowMultiQueries=true&serverTimezone=Asia/Shanghai";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        /*DataStream<String> streamSource = environment.readTextFile("flink/src/main/resources/sensor.txt");

        DataStream<SensorReading> sensorReadings = streamSource.map(s -> {
            String[] split = s.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });*/

        DataStreamSource<SensorReading> sensorReadings = environment.addSource(new SourceFunction<SensorReading>() {
            private boolean running = true;

            @Override
            public void run(SourceContext<SensorReading> sourceContext) throws Exception {
                Random random = new Random();
                HashMap<String, Double> sensorTempMap = new HashMap<>();
                for (int i = 0; i < 10; i++) {
                    sensorTempMap.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20);
                }
                while (running) {
                    for (String s : sensorTempMap.keySet()) {
                        Double newTmp = sensorTempMap.get(s) + random.nextGaussian();
                        sensorTempMap.put(s, newTmp);
                        sourceContext.collect(new SensorReading(s, System.currentTimeMillis(), newTmp));
                    }
                    Thread.sleep(1000L);
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });

        sensorReadings.addSink(new RichSinkFunction<SensorReading>() {

            Connection connection = null;
            PreparedStatement insertStatement = null;
            PreparedStatement updateStatement = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                connection = DriverManager.getConnection(JDBC_URL,"root","691244507Zk");
                insertStatement = connection.prepareStatement("insert into sensor_temp (sensor_id,temperature) values (?,?)");
                updateStatement = connection.prepareStatement("update sensor_temp set temperature = ? where sensor_id = ?");
            }

            @Override
            public void close() throws Exception {
                insertStatement.close();
                updateStatement.close();
                connection.close();
            }

            @Override
            public void invoke(SensorReading value, Context context) throws Exception {
                updateStatement.setDouble(1, value.getTemperature());
                updateStatement.setString(2, value.getId());
                updateStatement.execute();
                if (updateStatement.getUpdateCount() == 0){
                    insertStatement.setString(1, value.getId());
                    insertStatement.setDouble(2, value.getTemperature());
                    insertStatement.execute();
                }
            }
        });

        environment.execute();
    }
}
