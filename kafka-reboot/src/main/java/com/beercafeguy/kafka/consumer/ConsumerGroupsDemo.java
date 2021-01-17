package com.beercafeguy.kafka.consumer;

import com.beercafeguy.kafka.common.PropertyFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerGroupsDemo {

    private static Logger logger= LoggerFactory.getLogger(ConsumerGroupsDemo.class);

    private static String topicName="second_kafka_topic";
    public static void main(String[] args) {

        // Get Consumer Properties
        Properties consumerProperties= PropertyFactory.getConsumerProps();
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"AnalyticsAppGrp");

        // Created Consumer
        KafkaConsumer<String,String> kafkaConsumer= new KafkaConsumer<>(consumerProperties);

        // Subscribe to topic using consumer
        kafkaConsumer.subscribe(Collections.singletonList(topicName));

        //poll for data
        while(true){
            ConsumerRecords<String,String> consumerRecords=kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String,String> record:consumerRecords) {
                logger.info("\nKey: "+record.key() +"\n"
                +"Value : "+record.value()+"\n"+
                        "Partitions : "+record.partition()+"\n"+
                        "Offset: "+record.offset() +" \n");
            }
        }
    }
}
