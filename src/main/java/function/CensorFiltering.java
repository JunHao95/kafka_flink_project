package function;

import model.KafkaEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class CensorFiltering extends KeyedProcessFunction<String, KafkaEvent, KafkaEvent> {
    private static final Map<String, String> vulgaritiesMap = new HashMap<>();
    private transient ValueState<Integer> countState;
    private transient ValueState<Long> timerState;

    static {
      vulgaritiesMap.put("fuck", "f**k");
      vulgaritiesMap.put("knnccb", "k**c**");
      vulgaritiesMap.put("hell", "he11");
      vulgaritiesMap.put("Diu Lei Lou Mou", "DLLM");
    }

  @Override
  public void open(Configuration parameters) {
    ValueStateDescriptor<Integer> countDescriptor = new ValueStateDescriptor<>("messageCount", Types.INT, 0);
    countState = getRuntimeContext().getState(countDescriptor);
    System.out.println("countState initiatlised: "+ countState);

    ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>("timerState", Types.LONG);
    timerState = getRuntimeContext().getState(timerDescriptor);
    System.out.println("timerState initiatlised: "+ timerState);
  }

  @Override
  public void processElement(KafkaEvent value, KeyedProcessFunction<String, KafkaEvent, KafkaEvent>.Context ctx, Collector<KafkaEvent> out) throws Exception {
    // Sanitize the value
    String sanitizedValue = sanitizeValue(value.value); // map the expletives
    KafkaEvent sanitizedEvent = new KafkaEvent(value.key, sanitizedValue, value.timestamp);

    // Check and update count
    Integer currentCount = countState.value();
    countState.update(currentCount + 1);
    System.out.println("currentCount : "+ currentCount);

    // Check for spam and set/reset timer
    if (currentCount + 1 == 1) {
      // Set the timer for the first message in a window
      long timer = ctx.timerService().currentProcessingTime() + Time.minutes(1).toMilliseconds();
      ctx.timerService().registerProcessingTimeTimer(timer);
      timerState.update(timer);
    }
    if (currentCount <= 10) {
      out.collect(sanitizedEvent);
    } else {
      // Detected spam, emit a special event
      if (currentCount == 11) {
        KafkaEvent spamEvent = new KafkaEvent(value.key, "Spam detected. Pausing stream.", value.timestamp);
        out.collect(spamEvent);

        // we want to resume the stream after 5 seconds for beter user exp
        long resumeTimer = ctx.timerService().currentProcessingTime() + Time.minutes(5).toMilliseconds();
        ctx.timerService().registerProcessingTimeTimer(resumeTimer);
        timerState.update(resumeTimer); // store to distinguish in onTimer func
      }
    }
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<KafkaEvent> out) throws Exception{
    Long resumeTime = timerState.value();
    if (timestamp == resumeTime){
      countState.clear(); // clear the spam counter to resume the stream and start afresh
    } else {
      //reet count and timer state for end of window
      countState.clear();
      timerState.clear();
    }
  }

  private String sanitizeValue(String value) {
    for (Map.Entry<String, String> entry : vulgaritiesMap.entrySet()) {
      value = value.replaceAll(entry.getKey(), entry.getValue());
    }
    return value;
  }
}
