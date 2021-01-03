package com.beercafeguy.kafka.consumer;

import com.beercafeguy.kafka.common.PropertyFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerAssignSeekDemo {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerAssignSeekDemo.class);

    private static final String bootstrapServers = "localhost:9092";
    private static final String topicName = "second_kafka_topic";

    public static void main(String[] args) {

        // Get Consumer Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Created Consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        //assign
        TopicPartition partitionToReadFrom = new TopicPartition(topicName, 1);
        kafkaConsumer.assign(Collections.singletonList(partitionToReadFrom));

        //seek
        kafkaConsumer.seek(partitionToReadFrom, 4);

        int numberOfMessagesToRead = 10;
        int numberOfMessagesReadSoFar = 0;
        boolean keepReading = true;

        //poll for data
        while (keepReading) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : consumerRecords) {
                numberOfMessagesReadSoFar++;
                logger.info("\nKey: " + record.key() + "\n"
                        + "Value : " + record.value() + "\n" +
                        "Partitions : " + record.partition() + "\n" +
                        "Offset: " + record.offset() + " \n");
                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
                    keepReading = false;
                    break;
                }
            }
        }
    }
}
