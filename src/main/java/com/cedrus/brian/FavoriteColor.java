package com.cedrus.brian;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public class FavoriteColor {


    public static void main(String[] args) {
        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kstreams");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

//        KTable<String, String> rawTable = builder.table("input_topic");

//        rawTable.groupBy((key, value)-> {
//            return
//        })


        KStream<String, String> baseStream = builder.stream("input_topic");


        KGroupedStream<String, String> newGroup = baseStream.groupBy((key,val)-> {
            System.out.println(val);
            return val;
        });

        KTable finalTable = newGroup.count(Materialized.as("my-store"));

        finalTable.mapValues((key,val)->{
            System.out.printf("%s  %s \n", key,val);
            return val;
        });


//        KStream filtered = baseStream.filter((key, val) -> {
//            return val.contains("red") || val.contains("green") || val.contains("blue");
//        });
//        KStream<String, String> transformed = filtered.mapValues(val -> {
//            System.out.println(val.getClass().getName());
//            String [] split = ((String) val).split(",");
//            System.out.println(split[0]);
//            return val;
//        });

//        filtered.print(Printed.toSysOut());

        final Topology topo = builder.build();
        KafkaStreams streams = new KafkaStreams(topo, config);
        streams.start();
        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
