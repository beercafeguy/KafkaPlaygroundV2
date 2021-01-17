package com.beercafeguy.avro.kafka.consumer;

import com.beercafeguy.avro.commons.PropertyFactory;
import com.beercafeguy.avro.kafka.producer.KafkaAvroProduceDemo;
import com.example.CustomerSimple;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

public class KafkaAvroConsumerDemo {

    private static String topic="topic_customer_service";

    static Logger logger= LoggerFactory.getLogger(KafkaAvroConsumerDemo.class);

    public static void main(String[] args) {

        Properties properties= PropertyFactory.getConsumerProps();

        KafkaConsumer<String, CustomerSimple> kafkaConsumer=
                new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        System.out.println("Waiting for data");

        while(true){
            ConsumerRecords<String,CustomerSimple> consumerRecords=kafkaConsumer.poll(500);
            for(ConsumerRecord<String,CustomerSimple> record:consumerRecords){
                CustomerSimple customer=record.value();
                logger.info(customer.toString());
            }
            kafkaConsumer.commitSync();
        }

        //kafkaConsumer.close();
    }
}
