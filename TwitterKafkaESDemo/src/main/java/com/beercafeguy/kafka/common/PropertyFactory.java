package com.beercafeguy.kafka.common;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class PropertyFactory {
    private static String bootstrapServers="localhost:9092";
    private PropertyFactory() {
    }

    public static Properties getAppProperties() throws IOException {
        FileReader reader=new FileReader("src/main/resources/application.properties");
        Properties properties=new Properties();
        properties.load(reader);
        return properties;
    }

    public static Properties getProducerProps(){
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"1");
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        return properties;
    }
}
