package top.tzk.flink.bean;

public class SensorReading {
    private String id;
    private Long ts;
    private Double temperature;

    public SensorReading(String id, Long ts, Double temperature) {
        this.id = id;
        this.ts = ts;
        this.temperature = temperature;
    }

    public SensorReading() {
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "id='" + id + '\'' +
                ", timestamp=" + ts +
                ", temperature=" + temperature +
                '}';
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }
}
