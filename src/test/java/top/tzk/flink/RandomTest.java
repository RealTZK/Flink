package top.tzk.flink;

import top.tzk.flink.bean.SensorReading;

import java.util.Random;

public class RandomTest {
    public static void main(String[] args) {
        Random random = new Random();
        while (true){
            SensorReading sensorReading = new SensorReading(
                    String.valueOf(1001 + random.nextInt(10)),
                    System.currentTimeMillis() + Math.round(random.nextGaussian() * 500),
                    36.7 + random.nextGaussian() * 0.5
            );
            System.out.println(sensorReading);
        }
    }
}