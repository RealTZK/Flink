package top.tzk.flink.stream.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import top.tzk.flink.bean.SensorReading;
import top.tzk.flink.stream.source.MessageGenerator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class SinkTest4_JDBC {
    public final static String JDBC_URL = "";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<SensorReading> dataStream = environment.addSource(new MessageGenerator());
        dataStream.addSink(new RichSinkFunction<>() {
            private Connection connection;
            PreparedStatement insertStatement = null;
            PreparedStatement updateStatement = null;

            @Override
            public void invoke(SensorReading value, Context context) throws Exception {
                updateStatement.setDouble(1, value.getTemperature());
                updateStatement.setString(2, value.getId());
                updateStatement.execute();
                if (updateStatement.getUpdateCount() == 0) {
                    insertStatement.setDouble(2, value.getTemperature());
                    insertStatement.setString(1, value.getId());
                    insertStatement.execute();
                }
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                connection = DriverManager.getConnection(JDBC_URL, "user", "password");
                insertStatement = connection.prepareStatement(
                        "insert into " +
                                "sensor_temperature (sensor_id,temperature) " +
                                "values (?,?)");
                updateStatement = connection.prepareStatement(
                        "update sensor_temperature " +
                                "set temperature = ? " +
                                "where sensor_id = ?");
            }

            @Override
            public void close() throws Exception {
                insertStatement.close();
                updateStatement.close();
                connection.close();
            }
        });
        environment.execute();
    }
}
