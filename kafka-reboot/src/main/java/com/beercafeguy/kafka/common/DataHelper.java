package com.beercafeguy.kafka.common;

import org.apache.kafka.clients.producer.ProducerRecord;

import javax.xml.crypto.Data;

public class DataHelper {

    private DataHelper(){}

    public static ProducerRecord<String,String> getNoKeyProducerRecord(String topicName){
        ProducerRecord<String,String> producerRecord=new ProducerRecord<>(topicName,"value -> "+  Math.random()*10);
        return producerRecord;
    }
}
