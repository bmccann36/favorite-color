package com.github.simplenick.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

public class FavoriteColorApp {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "favorite-color-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> inputTopicStream = builder.stream("favorite-color-input");

        KStream<String, String> keyAddedStream = inputTopicStream.selectKey((key, val) -> {
           return val.split(",")[0];
        });

        KStream<String, String> valueUpdatedStream = keyAddedStream.mapValues((unusedKey, val) -> {
            return val.split(",")[1].toLowerCase();
        });

        KStream<String, String> filteredColorStream = valueUpdatedStream.filter((key, val) -> {
           return val.equals("green") || val.equals("blue") || val.equals("red");
        });

        filteredColorStream.to("intermediate-topic", Produced.with(Serdes.String(), Serdes.String())); // create in
        // order to publish stream to a k table to eliminate repeats

        KTable<String, String> rawTable = builder.table("intermediate-topic");

        KGroupedTable<String, String> groupedTable =
                rawTable.groupBy((key, value) -> new KeyValue<String, String>(value, value));

        KTable<String, Long> countColors = groupedTable.count(Materialized.as("count"));

        countColors.toStream().print(Printed.toSysOut());

        countColors.toStream().to("favorite-color-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }


}
