package com.beercafeguy.kafka.producer;

import com.beercafeguy.kafka.common.DataHelper;
import com.beercafeguy.kafka.common.PropertyFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        System.out.println("Hello Kafka!");
        String topicName="first_kafka_topic";
        // Get Producer Properties
        Properties producerProperties= PropertyFactory.getProducerProps();
        // Create Producer
        KafkaProducer<String,String> kafkaProducer=new KafkaProducer<String, String>(producerProperties);

        //Create Producer Record
        ProducerRecord<String,String> producerRecord= DataHelper.getNoKeyProducerRecord(topicName);
        //Publish to Kafka using Producer
        kafkaProducer.send(producerRecord);
        kafkaProducer.flush();
        kafkaProducer.close();
    }


}
