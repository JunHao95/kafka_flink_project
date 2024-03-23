package serdes;

import model.KafkaEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;


public class KafkaEventDeserializer implements KafkaRecordDeserializationSchema<KafkaEvent> {

  public static StringDeserializer stringDeserializer = new StringDeserializer();

    public String topic;

    public KafkaEventDeserializer() {}

    public KafkaEventDeserializer(String topic) { this.topic = topic; }


    @Override
    public TypeInformation<KafkaEvent> getProducedType() {
        return TypeInformation.of(KafkaEvent.class);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<KafkaEvent> out) throws IOException {
        String key = stringDeserializer.deserialize(topic, record.key());
        String value = stringDeserializer.deserialize(topic, record.value());
        long timestamp = record.timestamp();
        out.collect(new KafkaEvent(key, value, timestamp));
    }
}
