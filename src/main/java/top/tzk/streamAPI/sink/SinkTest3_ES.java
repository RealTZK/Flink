package top.tzk.streamAPI.sink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import top.tzk.streamAPI.beans.SensorReading;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @Author: tianzhenkun
 * @Date: 2021/2/5
 * @Description:
 * @Modified By:
 */
public class SinkTest3_ES {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStream<String> streamSource = environment.readTextFile("flink/src/main/resources/sensor.txt");

        DataStream<SensorReading> sensorReadings = streamSource.map(s -> {
            String[] split = s.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });

        // 定义es的连接配置
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("39.97.123.131", 9200));

        sensorReadings.addSink(new ElasticsearchSink.Builder<SensorReading>(httpHosts,
                (ElasticsearchSinkFunction<SensorReading>) (sensorReading, runtimeContext, requestIndexer) -> {
                    // 定义写入的数据source
                    HashMap<String, String> dataSource = new HashMap<>();
                    dataSource.put("id", sensorReading.getId());
                    dataSource.put("temp", sensorReading.getTemperature().toString());
                    dataSource.put("ts", sensorReading.getTimestamp().toString());

                    // 创建请求,作为向es发起的写入命令
                    IndexRequest indexRequest = Requests.indexRequest()
                            .index("sensor")
                            .type("readingData")
                            .source(dataSource);

                    // 用requestIndexer发送请求
                    requestIndexer.add(indexRequest);
                }).build());

        environment.execute();
    }
}