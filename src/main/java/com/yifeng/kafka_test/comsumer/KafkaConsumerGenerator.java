package com.yifeng.kafka_test.comsumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

/**
 * Created by guoyifeng on 11/14/18
 */
public class KafkaConsumerGenerator {
    private static Properties kafkaProps;

    static {
        kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092, localhost:9092");
        kafkaProps.put("group.id", "CountryCounter");
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }

    public static KafkaConsumer<String, String> createConsumer() {
        return new KafkaConsumer<String, String>(kafkaProps);
    }
}
