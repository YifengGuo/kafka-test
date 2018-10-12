package com.yifeng.kafka_test.producer;

/**
 * Created by guoyifeng on 10/12/18
 */

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.Properties;

/**
 * create an instance of kafka producer
 */
public class KafkaProducerGenerator {
    private static Properties kafkaProps;

    static {
        kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092, localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    public static KafkaProducer<String, String> createProducer() {
        return new KafkaProducer<String, String>(kafkaProps);
    }
}
