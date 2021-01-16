package com.beercafeguy.avro.kafka.producer;

import com.beercafeguy.avro.commons.PropertyFactory;
import com.example.CustomerSimple;
import com.example.CustomerV1;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaAvroProduceDemo {

    private static String topic="topic_customer_service";

    static Logger logger= LoggerFactory.getLogger(KafkaAvroProduceDemo.class);

    public static void main(String[] args) {

        Properties properties= PropertyFactory.getProducerProps();

        KafkaProducer<String, CustomerSimple> producer=new KafkaProducer<String, CustomerSimple>(properties);

        CustomerSimple customer=CustomerSimple.newBuilder()
                .setFirstName("Aman")
                .setLastName("John")
                .setAge(20)
                .setWeight(65f)
                .setHeight(169f)
                .setAutomatedEmail(false)
                .build();

        ProducerRecord<String,CustomerSimple> producerRecord=new ProducerRecord<String, CustomerSimple>(topic,"Aman",customer);
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e==null){
                    System.out.println("Success");
                    System.out.println(recordMetadata.toString());
                }else{
                    logger.error("Produce failed");
                }

            }
        });

        producer.flush();
        producer.close();
    }
}
