package com.yifeng.kafka_test.comsumer.manually_sync;

import com.yifeng.kafka_test.comsumer.KafkaConsumerAnalysis;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by guoyifeng on 4/28/20
 */
public class ManuallySyncCommit {
    private static final Logger LOG = LoggerFactory.getLogger(ManuallySyncCommit.class);

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
                for (TopicPartition partition : consumerRecords.partitions()) {  // consume msgs grouped by different partitions
                    List<ConsumerRecord<String, String>> partitionRecords = consumerRecords.records(partition);
                    for (ConsumerRecord record : partitionRecords) {
                        // do some business logic
                    }
                    // commit manually
                    long lastConsumedOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastConsumedOffset + 1)));
                }
            }
        } catch (Exception e) {
            LOG.error("error in consuming messages ", e);
        }
    }
}
