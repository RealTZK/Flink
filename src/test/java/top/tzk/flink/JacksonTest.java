package top.tzk.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import top.tzk.flink.bean.SensorReading;

import java.io.IOException;
import java.util.Map;

public class JacksonTest {
    public static void main(String[] args) throws IOException {
        SensorReading sensorReading = new SensorReading("1001", System.currentTimeMillis(), 36.7);
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(sensorReading);
        Map map = mapper.convertValue(sensorReading, Map.class);
        SensorReading reading = mapper.convertValue(map, SensorReading.class);
        System.out.println(13241);
    }
}
