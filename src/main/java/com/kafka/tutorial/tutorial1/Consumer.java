package com.kafka.tutorial.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
public class Consumer {

    private static Logger logger = LoggerFactory.getLogger(Consumer.class.getName());

    public static void main(String[] args) {
        Properties properties = new Properties();
        String server = "127.0.0.1:9092";
        String groupId = "my-fifth-application";
        String topic = "first_topic";

//        Create Consumer Config
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");// earliest/latest/none

//        Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

//        Subscribe consumer to topic(s)
        consumer.subscribe(Arrays.asList(topic));

//        poll data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record : records) {
                System.out.println(record.key());
                System.out.println(record.value());
                System.out.println(record.offset());
                System.out.println(record.partition());
                System.out.println(record.timestamp());
            }
        }
    }
}
