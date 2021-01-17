package com.beercafeguy.avro.commons;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class PropertyFactory {

    private static String bootstrapServers="127.0.0.1:9092";

    public static Properties getProducerProps(){
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"1");
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"none"); // no compression
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,"2147483647");
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG,"SampleProducerOne");
        properties.setProperty("schema.registry.url","http://127.0.0.1:8081");
        return properties;
    }

    public static Properties getConsumerProps(){
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"SampleConsumerGroupOne");
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG,"SampleConsumerOne");
        properties.setProperty("schema.registry.url","http://127.0.0.1:8081");
        properties.setProperty("specific.avro.reader","true");

        return properties;
    }
}
