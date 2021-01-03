package com.beercafeguy.kafka.producer;

import com.beercafeguy.kafka.common.DataHelper;
import com.beercafeguy.kafka.common.PropertyFactory;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallbackDemo {

    private static Logger logger= LoggerFactory.getLogger(ProducerWithCallbackDemo.class);

    public static void main(String[] args) {
        System.out.println("Hello Kafka!");
        String topicName="first_kafka_topic";
        // Get Producer Properties
        Properties producerProperties= PropertyFactory.getProducerProps();
        // Create Producer
        KafkaProducer<String,String> kafkaProducer=new KafkaProducer<String, String>(producerProperties);

        for (int i=0;i < 10;i++) {
            //Create Producer Record
            ProducerRecord<String, String> producerRecord = DataHelper.getNoKeyProducerRecord(topicName);
            //Publish to Kafka using Producer
            kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes everytime record is published or exception thrown
                    if (null == e) {
                        logger.info("Topic : " + recordMetadata.topic() + " |\n Partition: " + recordMetadata.partition()
                                + " |\n Offset: " + recordMetadata.offset() + " |\n Timestamp :" + recordMetadata.timestamp()+"\n");
                    } else {
                        logger.error("Error while producing : ", e);
                    }
                }
            });
        }
        kafkaProducer.flush();
        kafkaProducer.close();
    }


}
