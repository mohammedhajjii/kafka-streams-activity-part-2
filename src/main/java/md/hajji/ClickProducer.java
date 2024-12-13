package md.hajji;

import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

public class ClickProducer {

    static final String TOPIC = "clicks";
    static final List<String> USERS = List.of("Hassouni", "Hajji", "Oussama");
    static final String VALUE = "click";
    static final Random RANDOM = new Random();


    @SneakyThrows
    static ProducerRecord<String, String> click() {
        Thread.sleep(Duration.ofSeconds(2));
        return new ProducerRecord<>(
                TOPIC,
                USERS.get(RANDOM.nextInt(USERS.size())),
                VALUE
        );
    }



    public static void main(String[] args) {


        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        
        try( KafkaProducer<String, String> producer = new KafkaProducer<>(props)){

            Stream.generate(ClickProducer::click)
//                    .limit(20)
                    .map(producer::send)
                    .forEach(_ -> System.out.println("click sent"));

        }


    }
}
