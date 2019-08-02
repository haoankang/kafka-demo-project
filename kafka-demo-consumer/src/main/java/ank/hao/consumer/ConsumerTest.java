package ank.hao.consumer;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerTest {
    public static void main(String[] args) {
        String topic = "first-topic";
        String groupId = "first-group";

        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "10.100.1.130:9092");
        properties.put(GROUP_ID_CONFIG, groupId);
        properties.put(ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        try {
            while (true){
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofSeconds(1));
                for(ConsumerRecord<String,String> record:records){
                    System.out.printf("offset=%d, key=%s, value=%s%n", record.offset(), record.key(), record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
