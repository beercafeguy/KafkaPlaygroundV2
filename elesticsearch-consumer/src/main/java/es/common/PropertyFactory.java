package es.common;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class PropertyFactory {
    private static String bootstrapServers="localhost:9092";
    private PropertyFactory() {
    }

    public static Properties getAppProperties() throws IOException {
        FileReader reader=new FileReader("elesticsearch-consumer/src/main/resources/application.properties");
        Properties properties=new Properties();
        properties.load(reader);
        return properties;
    }

    public static Properties getAppProperties(String propFileName) throws IOException {
        FileReader reader=new FileReader("elesticsearch-consumer/src/main/resources/"+propFileName);
        Properties properties=new Properties();
        properties.load(reader);
        return properties;
    }

    public static Properties getConsumerProps(){
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"ESConsumerIdempGroupOne");
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");

        return properties;
    }

    public static Properties getManualCommitConsumerProps(){
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"ESConsumerManualGroupOne");
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"100");
        return properties;
    }
}
