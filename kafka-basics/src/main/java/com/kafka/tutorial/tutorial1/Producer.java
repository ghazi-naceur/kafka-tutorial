package com.kafka.tutorial.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

/**
 * Created by Ghazi Naceur on 06/05/2019
 * Email: ghazi.ennacer@gmail.com
 */
public class Producer {

    public static void main(String[] args) {
//        Create Producer properties
//        https://kafka.apache.org/documentation/#producerconfigs
        Properties properties = new Properties();
        String server = "127.0.0.1:9092";
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//        Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

//        Send Data - asynchronous
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hxh");
        producer.send(record);
        producer.flush(); // flush
        producer.close(); // flush and close
    }
}
