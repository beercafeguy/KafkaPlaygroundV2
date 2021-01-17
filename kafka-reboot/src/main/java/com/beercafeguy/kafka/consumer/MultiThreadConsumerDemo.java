package com.beercafeguy.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class MultiThreadConsumerDemo {
    private static final String topicName = "second_kafka_topic";

    public static void main(String[] args) {
        new MultiThreadConsumerDemo().run();
    }


    private void run() {
        final Logger logger = LoggerFactory.getLogger(MultiThreadConsumerDemo.class);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        logger.info("Creating consumer");
        Runnable myConsumer = new ConsumerThread(countDownLatch,
                "localhost:9092", "NotificationApp", "second_kafka_topic");
        Thread myThread = new Thread(myConsumer);
        myThread.start();
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("Application interrupted", e);
        } finally {
            logger.info("Application is closing.");
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught Shutdown.");
            ((ConsumerThread) myConsumer).shutDown();
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
    }

    private MultiThreadConsumerDemo() {
    }

    public class ConsumerThread implements Runnable {

        private Logger logger = LoggerFactory.getLogger(ConsumerThread.class);
        CountDownLatch countDownLatch;
        private KafkaConsumer<String, String> kafkaConsumer;

        public ConsumerThread(CountDownLatch countDownLatch,
                              String bootstrapServers,
                              String groupId,
                              String topic) {
            this.countDownLatch = countDownLatch;

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "SampleConsumerOne");

            kafkaConsumer = new KafkaConsumer<>(properties);
            kafkaConsumer.subscribe(Collections.singletonList(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : consumerRecords) {
                        logger.info("\nKey: " + record.key() + "\n"
                                + "Value : " + record.value() + "\n" +
                                "Partitions : " + record.partition() + "\n" +
                                "Offset: " + record.offset() + " \n");
                    }
                }
            } catch (WakeupException wakeupException) {
                logger.error("Received shutdown signal");
            } finally {
                kafkaConsumer.close();
                countDownLatch.countDown(); // tell the code that we are done with consumption
            }
        }

        public void shutDown() {
            kafkaConsumer.wakeup();
        }
    }
}
