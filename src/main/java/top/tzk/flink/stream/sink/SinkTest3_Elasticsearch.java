package top.tzk.flink.stream.sink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import top.tzk.flink.bean.SensorReading;
import top.tzk.flink.stream.source.MessageGenerator;
import top.tzk.flink.util.JsonUtil;

import java.util.List;

public class SinkTest3_Elasticsearch {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<SensorReading> dataStream = environment.addSource(new MessageGenerator());
        dataStream.addSink(new ElasticsearchSink.Builder<SensorReading>(
                List.of(new HttpHost("host", 0000)),
                (value, context, indexer) -> {
                    IndexRequest request = null;
                    request = Requests.indexRequest("sensor")
                            .source(JsonUtil.toJson(value));
                    indexer.add(request);
                }
        ).build());
        environment.execute();
    }
}
