package md.hajji;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class ClickCountConsumer {

    static final String TOPIC = "clicks-count";
    static final Long POLL_DELAY_SEC = 2L;

    static void log(ConsumerRecord<String, String> record) {
        System.out.println(record.key() + " : " + record.value());
    }

    static  void flatten(
            ConsumerRecords<String, String> records,
            Consumer<ConsumerRecord<String, String>> downstream){
        records.records(TOPIC).forEach(downstream);
    }


    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "click-count-consumer-groups");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());


        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

            consumer.subscribe(Collections.singletonList(TOPIC));
            Stream.generate(() -> consumer.poll(Duration.ofSeconds(POLL_DELAY_SEC)))
                    .mapMulti(ClickCountConsumer::flatten)
                    .forEach(ClickCountConsumer::log);

        }



    }
}
