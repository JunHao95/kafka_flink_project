import function.CensorFiltering;
import model.KafkaEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import serdes.KafkaEventDeserializer;
import serdes.KafkaEventSerializer;


public class Main {
    public static final String BOOTSTRAP_SERVERS = "kafka0:9094,kafka1:9094,kafka2:9092";
    public static final String SOURCE_TOPIC = "raw-conversation";
    public static final String SINK_TOPIC = "filtered-conversation";
    public static final String SPAM_ALERT = "spam_alert";



    public static void main(String[] args) throws Exception {
        // user balances
        KafkaSource<KafkaEvent> source = KafkaSource.<KafkaEvent>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(SOURCE_TOPIC)
                .setGroupId("flink-consumer-balances")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new KafkaEventDeserializer(SOURCE_TOPIC))
                .setProperty("partition.discovery.interval.ms", "60000")
                .build();

        // filtered messsage sink
        KafkaSink<KafkaEvent> sink = KafkaSink.<KafkaEvent>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(new KafkaEventSerializer(SINK_TOPIC))
                .setTransactionalIdPrefix("filter-messages")
                .build();
        // spam alert sink
      KafkaSink<KafkaEvent> alert = KafkaSink.<KafkaEvent>builder()
        .setBootstrapServers(BOOTSTRAP_SERVERS)
        .setRecordSerializer(new KafkaEventSerializer(SPAM_ALERT))
        .setTransactionalIdPrefix("spam-alert")
        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), SOURCE_TOPIC)
                .keyBy(x -> x.key)
                .process(new CensorFiltering())
                .sinkTo(sink);

        env.execute("Censor Chat Application");
    }
}
