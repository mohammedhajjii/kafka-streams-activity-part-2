
# Kafka streams app: Clicks counter example

## Application setups

### Docker-compose file


```yaml

services:

  # zookeeper:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.7.1
    container_name: zookeeper
    hostname: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - 22181:2181

    networks:
      - kafka-net
  
  # broker-1:
  broker-1:
    image: confluentinc/cp-kafka:7.7.1
    container_name: broker-1
    hostname: broker-1
    depends_on:
      - zookeeper
    ports:
      - 9091:29091
    environment:
      KAFKA_BROKER_ID: 0
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENERS: INTERNAL://:8081,EXTERNAL_SAME_HOST://:29091
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://broker-1:8081,EXTERNAL_SAME_HOST://localhost:9091
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL_SAME_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 2
      KAFKA_DEFAULT_REPLICATION_FACTOR: 2
      KAFKA_NUM_PARTITIONS: 3
    
    networks:
      - kafka-net

  

  broker-2:
    image: confluentinc/cp-kafka:7.7.1
    container_name: broker-2
    hostname: broker-2
    depends_on:
      - zookeeper
    ports:
      - 9092:29092
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENERS: INTERNAL://:8082,EXTERNAL_SAME_HOST://:29092
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://broker-2:8082,EXTERNAL_SAME_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL_SAME_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 2
      KAFKA_DEFAULT_REPLICATION_FACTOR: 2
      KAFKA_NUM_PARTITIONS: 3
    
    networks:
      - kafka-net


networks:
  kafka-net:
    driver: bridge
```


### ClickProducer

```java
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

```



### ClickStream

```java
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

```



### ClickCountConsumer

```java
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
```


