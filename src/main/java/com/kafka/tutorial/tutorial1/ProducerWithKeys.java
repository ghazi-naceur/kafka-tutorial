package com.kafka.tutorial.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

/**
 * Created by Ghazi Naceur on 06/05/2019
 * Email: ghazi.ennacer@gmail.com
 */
public class ProducerWithKeys {

    private static Logger logger = LoggerFactory.getLogger(ProducerWithKeys.class);

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
        for (int i = 0; i < 10; i++) {

            String topic = "first_topic";
            String value = "Message : " + i;
            String key = "id_" + i;

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            // Providing a key : same key goes to the same partition
            logger.info("key : "+ key);
            try {
                producer.send(record, (recordMetadata, e) -> {
                    if (e == null) {
                        logger.info("Record sent successfully.");
                        logger.info("Topic : " + recordMetadata.topic());
                        logger.info("Offset : " + recordMetadata.offset());
                        logger.info("Partition : " + recordMetadata.partition());
                        logger.info("Timestamp : " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error when trying to send record :", e);
                    }
                }).get(); // With get() we block the send to make it synchronous
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        producer.flush(); // flush
        producer.close(); // flush and close
    }
}
