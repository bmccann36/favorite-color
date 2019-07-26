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

        producer.send(new ProducerRecord("num_topic01", null, "un_stamped", 5l));
        producer.send(new ProducerRecord("num_topic01", null, 1551398400000l,"old_msg", 20l));
        producer.send(new ProducerRecord("num_topic01", null, 1451606400000l,"really_old_msg", 20l));



        producer.flush();
        producer.close();
    }
}
