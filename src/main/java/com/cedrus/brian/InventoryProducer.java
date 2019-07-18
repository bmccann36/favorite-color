package com.cedrus.brian;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


import java.time.LocalTime;
import java.util.Calendar;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class InventoryProducer {

    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        Long startTime = Calendar.getInstance().getTimeInMillis() - TimeUnit.MILLISECONDS.toMillis(5);

        ProducerRecord record1 = new ProducerRecord("inventory-topic02", null, startTime, "key01", "1", null);
        producer.send(record1);

        ProducerRecord record2 = new ProducerRecord("inventory-topic02", null, startTime + 10000, "key01", "2", null);
        producer.send(record2);
        ProducerRecord record2a = new ProducerRecord("inventory-topic02", null, startTime + 10010, "key01", "2a", null);
        producer.send(record2a);

        ProducerRecord record3 = new ProducerRecord("inventory-topic02", null, startTime + 20000, "key01", "3", null);
        producer.send(record3);

        ProducerRecord record4 = new ProducerRecord("inventory-topic02", null, startTime + 30000, "key01", "4", null);
        producer.send(record4);

        ProducerRecord record5 = new ProducerRecord("inventory-topic02", null, startTime + 40000, "key01", "5", null);
        producer.send(record5);

        producer.flush();
        producer.close();
    }

}
