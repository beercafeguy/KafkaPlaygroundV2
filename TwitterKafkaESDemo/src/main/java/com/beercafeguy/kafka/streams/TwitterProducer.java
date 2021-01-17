package com.beercafeguy.kafka.streams;

import com.beercafeguy.kafka.common.PropertyFactory;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    private String topicName="tp_twitter_covid_19_01";
    public TwitterProducer() {

    }

    public static void main(String[] args) {
        System.out.println("Hello Twitter!");
        try {
            new TwitterProducer().run();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() throws IOException {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
        //create twitter client
        Client hosebirdClient = getTwitterClient(msgQueue);
        // Attempts to establish a connection.
        hosebirdClient.connect();
        // create kafka producer

        KafkaProducer<String, String> kafkaProducer =
                new KafkaProducer<>(PropertyFactory.getProducerProps());

        Runtime.getRuntime().addShutdownHook(new Thread(() ->{
            logger.info("Stop order received");
            logger.info("Stopping twitter client.....");
            hosebirdClient.stop();
            logger.info("Closing kafka producer ...");
            kafkaProducer.close();
            logger.info("Done");
        }));
        // send tweets to kafka
        while (!hosebirdClient.isDone()) {
            try {
                String msg = msgQueue.poll(5, TimeUnit.SECONDS);
                if (msg != null) {
                    ProducerRecord<String,String> record=new ProducerRecord<>(topicName,msg);
                    logger.info("Message : " + msg);
                    kafkaProducer.send(record, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (null == e) {
                                logger.info("Topic : " + recordMetadata.topic() + " |\n Partition: " + recordMetadata.partition()
                                        + " |\n Offset: " + recordMetadata.offset() + " |\n Timestamp :" + recordMetadata.timestamp()+"\n");
                            } else {
                                logger.error("Error while producing : ", e);
                            }
                        }
                    });
                    kafkaProducer.flush();
                }
            } catch (InterruptedException e) {
                logger.error("Connection interrupted: ", e);
                hosebirdClient.stop();
            }
        }
        logger.info("End of App!");
    }

    public Client getTwitterClient(BlockingQueue<String> msgQueue) throws IOException {


        /** Declare the host you want to connect to,
         * the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        //List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("COVID19","kafka","USA");
        //hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Properties appProperties = PropertyFactory.getAppProperties();
        Authentication hosebirdAuth = new OAuth1(
                appProperties.getProperty("twitter.key"),
                appProperties.getProperty("twitter.secret_key"),
                appProperties.getProperty("twitter.token"),
                appProperties.getProperty("twitter.secret_token")
        );

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
        //.eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }
}
