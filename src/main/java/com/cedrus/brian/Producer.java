package com.cedrus.brian;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class Producer {

    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        producer.send(new ProducerRecord<>("input_topic", "stephane", "blue"));
        producer.send(new ProducerRecord<>("input_topic","bob", "purple"));
        producer.send(new ProducerRecord<>("input_topic", "john", "green"));
        producer.send(new ProducerRecord<>("input_topic", "stephane","red"));
        producer.send(new ProducerRecord<>("input_topic", "alice","red"));


        producer.flush();
        producer.close();
    }
}
