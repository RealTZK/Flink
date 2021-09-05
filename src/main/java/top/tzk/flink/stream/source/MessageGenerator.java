package top.tzk.flink.stream.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import top.tzk.flink.bean.SensorReading;

import java.text.DecimalFormat;
import java.util.Random;

public class MessageGenerator implements SourceFunction<SensorReading> {
    private boolean running = true;

    @Override
    public void run(SourceContext<SensorReading> context) throws Exception {
        Random random = new Random();
        random.nextGaussian();
        DecimalFormat format = new DecimalFormat("0.00");
        while (running) {
            context.collect(new SensorReading(
                    String.valueOf(1001 + random.nextInt(10)),
                    System.currentTimeMillis() + Math.round(random.nextGaussian() * 500),
                    Double.parseDouble(format.format(36.5 + random.nextGaussian() * 0.5))
            ));
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
