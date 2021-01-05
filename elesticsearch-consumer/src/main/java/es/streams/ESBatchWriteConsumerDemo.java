package es.streams;

import com.google.gson.JsonParser;
import es.common.ESClientFactory;
import es.common.PropertyFactory;
import es.common.RequestType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ESBatchWriteConsumerDemo {

    private static final String topicName = "tp_twitter_covid_19_01";
    private static final Logger logger = LoggerFactory.getLogger(ESBatchWriteConsumerDemo.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        Properties properties = PropertyFactory.getAppProperties("application-local.properties");
        RestHighLevelClient client = ESClientFactory.createClient(RequestType.LOCAL);


        KafkaConsumer<String, String> kafkaConsumer = createConsumer();
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
            logger.info("Received " + consumerRecords.count() + " records");

            if (consumerRecords.count() > 0) {
                BulkRequest bulkRequest = new BulkRequest();
                for (ConsumerRecord<String, String> record : consumerRecords) {

                    // For Idempotence, we need to pass id to elastic search
                    // 1.Kafka Generated ID
                    // String recordId = record.topic() + "_" + record.partition() + "_" + record.offset();

                    // 2. ID from Twitter
                    String tweetId = extractIdFromTweet(record.value());
                    String jsonString = record.value();
                    IndexRequest indexRequest = new IndexRequest(
                            properties.getProperty("es.index.name"),
                            "_doc",
                            tweetId
                    ).source(jsonString, XContentType.JSON);
                    bulkRequest.add(indexRequest);
                }
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Committing offset.....");
                kafkaConsumer.commitSync();
                logger.info("Offsets committed ....");
                //Thread.sleep(1000);
            }
        }
        //client.close();
    }

    private static JsonParser jsonParser = new JsonParser();

    private static String extractIdFromTweet(String value) {
        return jsonParser.parse(value).getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    private static KafkaConsumer<String, String> createConsumer() {
        Properties consumerProperties = PropertyFactory.getManualCommitConsumerProps();
        // Created Consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties);
        kafkaConsumer.subscribe(Collections.singletonList(topicName));
        return kafkaConsumer;
    }
}
