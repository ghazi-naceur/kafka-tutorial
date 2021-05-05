package streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

public class WordCountExample {

    public static void main(String[] args) {
        // Create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-kafka-streams");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // Create a Topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // Input topic
        KStream<String, String> streamInputTopic = streamsBuilder.stream("streams-plaintext-input");
        KTable<String, Long> count = streamInputTopic
                .mapValues((ValueMapper<String, String>) String::toLowerCase)
                .flatMapValues(v -> Arrays.asList(v.split(" ")))
                .selectKey((k, v) -> v)
                .groupByKey()
                .count(Materialized.as("Counts"));

        count.toStream().to("streams-wordcount-output2", Produced.with(Serdes.String(), Serdes.Long()));

        // Build the Topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        // Start streams app
        kafkaStreams.start();
    }
}
