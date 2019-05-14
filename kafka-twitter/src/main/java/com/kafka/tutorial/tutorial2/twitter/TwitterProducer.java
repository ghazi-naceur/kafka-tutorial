package com.kafka.tutorial.tutorial2.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

/**
 * Created by Ghazi Naceur on 09/05/2019
 * Email: ghazi.ennacer@gmail.com
 */
public class TwitterProducer {

    public TwitterProducer() {
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        // Create twitter client
        Client client = createTwitterClient(msgQueue);
        client.connect();

        // Create Kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // Adding a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Stopping application ...");
            client.stop();
            producer.close();
            System.out.println("Stopped");
        }));

        // Loop to send tweets to Kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                System.out.println(msg);
                /**
                 * Creating the topic "twitter_tweets" :
                 * kafka-topics --zookeeper 127.0.0.1:2181 --create --topic twitter_tweets --partitions 6 --replication-factor 1
                 *
                 * launching a consumer :
                 * kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic twitter_tweets
                 */
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), (recordMetadata, e) -> {
                    if (e != null) {
                        System.out.println("ERROR , : " + e);
                    }
                });
            }
        }
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {

        // Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        //        List<Long> followings = Lists.newArrayList(1234L, 566788L);
        //        hosebirdEndpoint.followings(followings);
        List<String> terms = Lists.newArrayList("kafka", "bitcoin", "java", "scala", "nosql", "bigdata");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        // https://developer.twitter.com/en/apps/16353728
        String consumerKey = "20C42BMN61ygWYBCrt62x0yah";
        String consumerSecret = "yRjobUnWPsKtL0NUMsxZ8C9RgJUQccncJB7shqYMhQsyqT03ug";
        String token = "174629489-rXxhQBS7G54rCaqMUYbPhpXugSTUTxBm8GN4uzTW";
        String tokenSecret = "tVrKYJFKxEwq1EI741m7GWgvIxU1mj2v1RUeJoMIo1XS3";
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")  // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        // Create Producer
        Properties properties = new Properties();
        String server = "127.0.0.1:9092";
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Safe Producer
        properties.setProperty(ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ACKS_CONFIG, "all");
        properties.setProperty(RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // high throughput producer (at the expense of a bit latency and CPU usage)
        properties.setProperty(COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(LINGER_MS_CONFIG, "20");
        properties.setProperty(BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); // 32 KB batch size

        return new KafkaProducer<>(properties);
    }
}
