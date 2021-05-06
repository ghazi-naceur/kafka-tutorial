package com.tuto.kafka.streams;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamsFilterTweets {

    Logger logger = LoggerFactory.getLogger(StreamsFilterTweets.class.getName());

    private static final JsonParser jsonParser = new JsonParser();

    public static Integer extractUserFollowersInTweet(String jsonTweet) {
                try {
                    return jsonParser
                            .parse(jsonTweet)
                            .getAsJsonObject()
                            .get("user")
                            .getAsJsonObject()
                            .get("followers_count")
                            .getAsInt();
                } catch (NullPointerException npe) {
                    return 0;
                }
    }

    public static void main(String[] args) {
        // Create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // Create a Topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // Input topic
        KStream<String, String> streamInputTopic = streamsBuilder.stream("twitter_topic");
        KStream<String, String> filteredResult = streamInputTopic.filter((k, jsonTweet) -> extractUserFollowersInTweet(jsonTweet) > 100);
        filteredResult.to("important_tweets");

        // Build the Topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        // Start streams app
        kafkaStreams.start();
    }
}
