package com.yifeng.kafka_test.comsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by guoyifeng on 4/17/20
 */
public class KafkaConsumerAnalysis {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerAnalysis.class);

    private static final String BROKER_LIST = "localhost:9092";

    private static final String TOPIC = "demo-topic";

    private static final String GROUP_ID = "demo-group";

    // to track the source of requests beyond just ip and port by allowing a logical application name to be included
    // in Kafka logs and monitoring aggregates
    // if not set, default value would be like consumer-1, consumer-2...
    private static final String CLIENT_ID = "demo-client-id";

    private static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        return properties;
    }


    public static void main(String[] args) {
        Properties properties = initConfig();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {  // close consumer in try
            consumer.subscribe(Arrays.asList(TOPIC));
            while (isRunning.get()) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord record : consumerRecords) {
                    System.out.println("topic = " + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset());
                    System.out.println("key = " + record.key() + ", value = " + record.value());
                    // business logic
                }
            }
        } catch (Exception e) {
            LOG.error("error in consuming messages ", e);
        }
    }
}
