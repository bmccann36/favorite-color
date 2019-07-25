package com.cedrus.brian;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
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
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());


        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

//        long time = 1554076800000l;

//        producer.send(new ProducerRecord("num_topic01", null, 1563926400000l,"key_in", 5l));
        producer.send(new ProducerRecord("num_topic01", null, 1563926400000l,"key_in", 5l)); // seems to work when timestamp is within range
        producer.send(new ProducerRecord("num_topic01", null, 1563926600000l,"key_in", 5l));
        producer.send(new ProducerRecord("num_topic01", null, 1563926700000l,"key_in", 5l));
        producer.send(new ProducerRecord("num_topic01", null, 1563926800000l,"key_in", 5l));


        producer.flush();
        producer.close();
    }
}
