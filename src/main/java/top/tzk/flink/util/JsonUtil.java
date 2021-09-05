package top.tzk.flink.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import top.tzk.flink.exception.JsonDeserializeException;
import top.tzk.flink.exception.JsonSerializeException;

import java.text.MessageFormat;
import java.util.Map;

public class JsonUtil {
    private JsonUtil() {
        throw new AssertionError("拒绝创建该实例!");
    }

    private static final ObjectMapper mapper;

    static {
        mapper = new ObjectMapper();
        // 在反序列化时忽略在 json 中存在但 Java 对象不存在的属性
        mapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        // 在序列化时忽略值为 null 的属性
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    public static <T> String toJson(T value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new JsonSerializeException(
                    MessageFormat.format(
                            "Json序列化异常,value:{0},type:{1}",
                            value,
                            value.getClass().getName()),
                    e
            );
        }
    }

    public static <T> T fromJson(String value, Class<T> type) {
        try {
            return mapper.readValue(value, type);
        } catch (JsonProcessingException e) {
            throw new JsonDeserializeException(
                    MessageFormat.format(
                            "Json反序列化异常,value:{0},type:{1}",
                            value,
                            type.getName()),
                    e
            );
        }
    }

    public static <T> T fromMap(Map map, Class<T> type) {
        return mapper.convertValue(map, type);
    }

    public static <T> Map toMap(T value) {
        return mapper.convertValue(value, Map.class);
    }
}
