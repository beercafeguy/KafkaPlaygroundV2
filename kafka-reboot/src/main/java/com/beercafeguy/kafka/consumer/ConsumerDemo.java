package com.beercafeguy.kafka.consumer;

import com.beercafeguy.kafka.common.PropertyFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    private static Logger logger= LoggerFactory.getLogger(ConsumerDemo.class);

    private static String topicName="second_kafka_topic";
    public static void main(String[] args) {

        // Get Consumer Properties
        Properties consumerProperties= PropertyFactory.getConsumerProps();

        // Created Consumer
        KafkaConsumer<String,String> kafkaConsumer=new KafkaConsumer<String, String>(consumerProperties);

        // Subscribe to topic using consumer
        kafkaConsumer.subscribe(Arrays.asList(topicName));

        //poll for data
        while(true){
            ConsumerRecords<String,String> consumerRecords=kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String,String> record:consumerRecords) {
                logger.info("\nKey: "+record.key() +"\n"
                +"Value : "+record.value()+"\n"+
                        "Partitions : "+record.partition()+"\n"+
                        "Offset: "+record.offset());
            }
        }
    }
}
