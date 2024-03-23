package function;

import model.KafkaEvent;
//import org.apache.flink.api.common.state.ValueState;
//import org.apache.flink.api.common.state.ValueStateDescriptor;
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class CensorFiltering extends KeyedProcessFunction<String, KafkaEvent, KafkaEvent> {
    private static final Map<String, String> vulgaritiesMap = new HashMap<>();

    static {
      vulgaritiesMap.put("fuck", "f**k");
      vulgaritiesMap.put("knnccb", "k**c**");
      vulgaritiesMap.put("hell", "he11");
      vulgaritiesMap.put("Diu Lei Lou Mou", "DLLM");
    }

//    @Override
//    public void open(Configuration parameters) {
//        ValueStateDescriptor<Boolean> isLastTxnSmallDescriptor = new ValueStateDescriptor<>("isLastTxnSmall", Types.BOOLEAN);
//        isLastTxnSmallKeyedValue = getRuntimeContext().getState(isLastTxnSmallDescriptor);
//    }

    @Override
    public void processElement(KafkaEvent value, KeyedProcessFunction<String, KafkaEvent, KafkaEvent>.Context ctx, Collector<KafkaEvent> out) throws Exception {
      String sanitizedValue = sanitizeValue(value.value);
      KafkaEvent sanitizedEvent = new KafkaEvent(value.key, sanitizedValue, value.timestamp);
      out.collect(sanitizedEvent);
    }

  private String sanitizeValue(String value) {
    for (Map.Entry<String, String> entry : vulgaritiesMap.entrySet()) {
      value = value.replaceAll(entry.getKey(), entry.getValue());
    }
    return value;
  }
}
