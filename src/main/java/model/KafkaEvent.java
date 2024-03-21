package model;

public class KafkaEvent {
  // just store the events data
    public String key;
    public String value;
    public long timestamp;
    public KafkaEvent() {
        this("", "", 0);
    }
    public KafkaEvent(String key, String value, long timestamp) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

}
