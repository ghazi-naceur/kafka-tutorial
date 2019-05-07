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
public class ConsumerGroups {

    private static Logger logger = LoggerFactory.getLogger(ConsumerGroups.class.getName());

//    This demo consists of running 2 consumers in order to see the rebalancing of partitions between consumers
//    Example : 1 consumer reads from 3 partitions
//              2 consumers : 1st one reads from 2 partitions and the 2nd one read from the last partition
//              3 consumers : each consumer reads from a specific partition
    public static void main(String[] args) {
        Properties properties = new Properties();
        String server = "127.0.0.1:9092";
        String groupId = "my-fifth-application"; // If you want to see early messages again, we can change the group name
                                                //      because the old group has already updated its offsets, and with
                                                //      the new group you will have offsets that start with 0.
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
