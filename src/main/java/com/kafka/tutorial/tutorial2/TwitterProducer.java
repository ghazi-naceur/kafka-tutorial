package com.kafka.tutorial.tutorial2;

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

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

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
        List<String> terms = Lists.newArrayList("bitcoin");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
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
}
