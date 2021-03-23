package ir.codefather;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerSeekDemo {

    static Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "java_application");
        // properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        TopicPartition topicPartition = new TopicPartition("first_topic", 0);
        kafkaConsumer.assign(Arrays.asList(topicPartition));
        kafkaConsumer.seek(topicPartition, 15);
        
        ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));

        for (ConsumerRecord<String, String> record : records) {
            logger.info("record info: partition:" + record.partition() + " - offset:" + record.offset());
            logger.info("record info: key:" + record.key() + " - value:" + record.value());
        }

        kafkaConsumer.close();
    }

}
