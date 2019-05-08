package com.kafka.tutorial.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created by Ghazi Naceur on 07/05/2019
 * Email: ghazi.ennacer@gmail.com
 */
public class ConsumerWithAssignAndSeek {

    private static Logger logger = LoggerFactory.getLogger(ConsumerWithAssignAndSeek.class.getName());

    public static void main(String[] args) {
        Properties properties = new Properties();
        String server = "127.0.0.1:9092";
        String topic = "first_topic";

//        Create Consumer Config
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");// earliest/latest/none

//        Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Assign and Seek are mostly used to reply data and fetch a specific message

        // Assign
        TopicPartition topicToReadFrom = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(topicToReadFrom));

        // Seek
        long offsetToReadFrom = 8L;
        consumer.seek(topicToReadFrom, offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;

//        poll data
        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record : records) {
                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead){
                    keepOnReading = false;
                    break;
                } else {
                    numberOfMessagesReadSoFar++;
                    logger.info("Key : "+ record.key());
                    logger.info("Value : "+ record.value());
                    logger.info("Offset : "+ record.offset());
                    logger.info("Partition : "+ record.partition());
                    logger.info("Timestamp : "+ record.timestamp());
                }
            }
        }
        logger.info("Exiting the application");
    }
}
