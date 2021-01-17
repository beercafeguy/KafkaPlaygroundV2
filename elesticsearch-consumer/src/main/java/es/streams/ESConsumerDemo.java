package es.streams;

import es.common.ESClientFactory;
import es.common.PropertyFactory;
import es.common.RequestType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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

public class ESConsumerDemo {

    private static final String topicName="tp_twitter_covid_19_01";
    private static final Logger logger= LoggerFactory.getLogger(ESConsumerDemo.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        Properties properties= PropertyFactory.getAppProperties("application-local.properties");
        RestHighLevelClient client= ESClientFactory.createClient(RequestType.LOCAL);



        KafkaConsumer<String,String> kafkaConsumer=createConsumer();
        while(true){
            ConsumerRecords<String,String> consumerRecords=kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String,String> record:consumerRecords) {

                String jsonString=record.value();
                IndexRequest indexRequest=new IndexRequest(
                        properties.getProperty("es.index.name")
                ).source(jsonString, XContentType.JSON);

                IndexResponse indexResponse=client.index(indexRequest, RequestOptions.DEFAULT);
                String id=indexResponse.getId();

                logger.info("ID -> "+id);
                logger.info("\nKey: "+record.key() +"\n"
                        +"Value : "+record.value()+"\n"+
                        "Partitions : "+record.partition()+"\n"+
                        "Offset: "+record.offset() +" \n");
                Thread.sleep(1000);
            }
        }
        //client.close();
    }

    private static KafkaConsumer<String,String> createConsumer(){
        Properties consumerProperties= PropertyFactory.getConsumerProps();
        // Created Consumer
        KafkaConsumer<String,String> kafkaConsumer= new KafkaConsumer<>(consumerProperties);
        kafkaConsumer.subscribe(Collections.singletonList(topicName));
        return kafkaConsumer;
    }
}
