package com.kafka.tutorial.tutorial3.elasticsearch;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Ghazi Naceur on 14/05/2019
 * Email: ghazi.ennacer@gmail.com
 */
public class ElasticsearchConsumer {

    public static void main(String[] args) throws IOException {

        RestHighLevelClient client = createClient();

        /**
         * Need to run :
         PUT twitter/_settings
         {
         "index.mapping.depth.limit": 1000,
         "index.mapping.total_fields.limit": 2000
         }

         * To avoid :
         * "type":"illegal_argument_exception","reason":"Limit of mapping
         *  depth [20] in index [twitter] has been exceeded due to object field
         *
         *  and
         *
         *  "type":"illegal_argument_exception","reason":"Limit of total
         *  fields [1000] in index [twitter] has been exceeded"
         */

        KafkaConsumer<String, String> consumer = createKafkaConsumer("twitter_tweets");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            System.out.println("Received records : " + records.count());
            for (ConsumerRecord record : records) {
                // String id = record.topic() + "_" + record.partition() + "_" + record.offset();
                Map<String, Object> source = getJsonStringAsMap(record);
                String id_str = source.get("id_str").toString(); // The aim of inserting a known id is to make the consumer idempotent
                // So if we want to restart our program, it will not reindex the same document twice, because it is already indexed with id_str
                IndexRequest request = new IndexRequest("twitter", "tweets", id_str)
                        .source(source, XContentType.JSON);
                IndexResponse response = client.index(request, RequestOptions.DEFAULT);
                System.out.println(response.getId());
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("Committing the offsets... ");
            consumer.commitSync();
            System.out.println("Offsets have been committed");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


//        client.close();
    }

    private static Map<String, Object> getJsonStringAsMap(ConsumerRecord record) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        TypeReference valueTypeRef = new TypeReference<Map<String, Object>>() {
        };
        return objectMapper.readValue(String.valueOf(record.value()), valueTypeRef);
    }

    private static RestHighLevelClient createClient() {
        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200));
        return new RestHighLevelClient(builder);
    }

    private static KafkaConsumer<String, String> createKafkaConsumer(String topic) {
        Properties properties = new Properties();
        String server = "127.0.0.1:9092";
        String groupId = "kafka-elasticsearch";

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }
}
