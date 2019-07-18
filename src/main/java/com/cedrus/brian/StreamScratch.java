package com.cedrus.brian;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;
import groovy.util.logging.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;


@Slf4j
public class StreamScratch {
    public void start() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-03");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        StreamsBuilder builder = new StreamsBuilder();
        // Step 1: We create the topic of users keys to colours
        KStream<String, String> initialStream = builder.stream("inventory-topic02");

        final KGroupedStream<String, String> groupedStream = initialStream.groupByKey();

        Materialized<String, String, WindowStore<Bytes, byte[]>> materializedAttritionStore = Materialized.as("windowedStore");

        final Windows<TimeWindow> timeWindow = TimeWindows.of(Duration.ofMillis(10000));

        final TimeWindowedKStream<String, String> tenSecWindowStream =
                groupedStream.windowedBy(timeWindow);

        KTable myTable = tenSecWindowStream.aggregate(() -> "", getAggregator(), materializedAttritionStore);

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        KafkaStreams.StateListener stateListener = new KafkaStreams.StateListener() {
            @Override
            public void onChange(KafkaStreams.State newState, KafkaStreams.State oldState) {
                System.out.println(newState);
              if(newState == KafkaStreams.State.RUNNING){
                  accessStore(streams);
              }
            }
        };
        streams.setStateListener(stateListener);

//        // only do this in dev - not in prod
        streams.cleanUp();
        streams.start();
        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }


        private void accessStore(KafkaStreams streams) {
            ReadOnlyWindowStore<String, String> stateStore = streams.store("windowedStore", QueryableStoreTypes.windowStore());
            Instant from = Instant.now().minus(2, ChronoUnit.MINUTES);
            Instant to = Instant.now();

            WindowStoreIterator<String> windowIterator = stateStore.fetch("key01", from, to);
            while (windowIterator.hasNext()) {
                KeyValue<Long, String> keyValue = windowIterator.next();
                // Get the interval record and parse it
                System.out.println("key:  " + keyValue.key + "  val:  " + keyValue.value);

            }
        }


    private static Aggregator<String, String, String> getAggregator() {
        return (key, val, aggregate) -> {
//            System.out.println("key is: " + key);
//            System.out.println("aggregate is: " + aggregate);
            aggregate = aggregate + " - - " + val;
//            System.out.println("after adding aggregate is: " + aggregate);
            return aggregate;
        };

    }


}