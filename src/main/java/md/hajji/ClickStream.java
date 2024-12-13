package md.hajji;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import java.util.Properties;

public class ClickStream {

    static final String INPUT_TOPIC = "clicks";
    static final String OUTPUT_TOPIC = "clicks-count";


    static <V> void log(String key, V value){
        System.out.println("{" + key + ": " + value + "}");
    }


    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "click-count-1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .peek(ClickStream::log)
                .groupByKey()
                .count()
                .toStream()
                .peek(ClickStream::log)
                .mapValues(String::valueOf)
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        final KafkaStreams INSTANCE = new KafkaStreams(builder.build(), props);
        INSTANCE.start();
        Runtime.getRuntime().addShutdownHook(new Thread(INSTANCE::close));

    }
}
