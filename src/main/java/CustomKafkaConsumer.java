import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import model.ContainerMelding;

public class CustomKafkaConsumer {
    private final static String TOPIC = "test";
    private final static String BOOTSTRAP_SERVER = "localhost:9092";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVER);
        props.put("group.id", "test");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("value.deserializer", "model.ContainerMeldingDeserializer");

        KafkaConsumer<Integer, ContainerMelding> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));
        while (true) {
            ConsumerRecords<Integer, ContainerMelding> records = consumer.poll(100);
            for (ConsumerRecord<Integer, ContainerMelding> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }

    }
}
