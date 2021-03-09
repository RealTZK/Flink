package top.tzk.streamAPI.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import top.tzk.streamAPI.beans.SensorReading;

import java.util.HashMap;
import java.util.Random;

/**
 * @Author: tianzhenkun
 * @Date: 2021/2/3
 * @Description:
 * @Modified By:
 */
public class SourceTest4_UDF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> source = environment.addSource(new SourceFunction<SensorReading>() {
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
        source.print();
        environment.execute();
    }
}
