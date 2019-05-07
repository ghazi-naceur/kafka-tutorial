package com.kafka.tutorial.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Created by Ghazi Naceur on 07/05/2019
 * Email: ghazi.ennacer@gmail.com
 */
public class ConsumerWithThread {

    public static void main(String[] args) {
        new ConsumerWithThread().launch();
    }

    private void launch() {
        String server = "127.0.0.1:9092";
        String groupId = "my-fifth-application";
        String topic = "first_topic";

        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // Create consumer runnable
        ConsumerRunnable consumerRunnable = new ConsumerRunnable(server, groupId, topic, latch);

        // Create thread
        Thread thread = new Thread(consumerRunnable);

        // Start thread
        thread.start();

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Caught shutdown hook");
            consumerRunnable.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Application is down");
        }));
        try {
            latch.await(); // makes us wait until the application is over
        } catch (InterruptedException e) {
            System.out.println("Application is interrupted");
            e.printStackTrace();
        } finally {
            System.out.println("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        Properties properties = new Properties();
        private CountDownLatch latch;
        KafkaConsumer<String, String> consumer;

        // CountDownLatch : in order to deal with concurrency
        // We will use it to shutdown our application properly
        public ConsumerRunnable(String server, String groupId, String topic, CountDownLatch latch) {

            this.latch = latch;

            // Create Consumer Config
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");// earliest/latest/none
            consumer = new KafkaConsumer<>(properties);

            // Subscribe consumer to topic(s)
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            // poll data
            try {
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
            } catch (WakeupException e) {
                logger.error("The consumer has wakeup");
            } finally {
                consumer.close();
                latch.countDown();// main code should exit / we're down with the consumer
//                shutdown();
            }
        }

        public void shutdown() {
            consumer.wakeup(); // interrupts consumer.poll() throwing the WakeupException
        }
    }
}
