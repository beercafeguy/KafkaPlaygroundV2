package com.beercafeguy.kafka.streams;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class FilterTweetsApp {

    private static final String bootstrapServer="localhost:9092";

    private static final String sourceTopic="tp_twitter_covid_19_01";
    private static final String targetTopic="tp_twitter_covid_max_reach";

    public static void main(String[] args) {
        Properties properties=new Properties();

        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"TweetFilterApp");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        //create streams

        StreamsBuilder streamsBuilder=new StreamsBuilder();

        //add input topic
        KStream<String,String> inputStream=streamsBuilder.stream(sourceTopic);
        KStream<String,String> filteredStream=inputStream.filter(
                (key,jsonTweet) -> numReweets(jsonTweet) > 1000
        );

        filteredStream.to(targetTopic);

        //build topology

        KafkaStreams kafkaStreams=new KafkaStreams(
                streamsBuilder.build(),
                properties
        );

        //start the app
        kafkaStreams.start();
    }

    private static JsonParser jsonParser=new JsonParser();
    private static long numReweets(String tweet){
        try {
            return jsonParser.parse(tweet)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsLong();
        }catch(NullPointerException e){
            return 0;
        }
    }

}
