package com.yifeng.kafka_test.comsumer.rebalance;

import com.yifeng.kafka_test.comsumer.seek.SeekExample;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by guoyifeng on 4/29/20
 *
 * applying ConsumerRebalanceListener with external store for offsets
 * to avoid duplicate data after rebalancing
 */
public class AfterRebalance {

    private static final Logger LOG = LoggerFactory.getLogger(SeekExample.class);

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
            Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();  // manually maintain committed offset on each partition
            consumer.subscribe(Arrays.asList(TOPIC), new ConsumerRebalanceListener() {
                /**
                 * This method will be called before a rebalance operation starts and after the consumer
                 * stops fetching data. It is recommended that offsets should be committed in this callback to either
                 * Kafka or a custom offset store to prevent duplicate data (consumption).
                 * @param partitions assigned partitions before rebalance
                 */
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    // store offsets into external store
                    // storeOffsetToDB();
                }

                /**
                 * A callback method the user can implement to provide handling of customized offsets on completion of a successful
                 * partition re-assignment. This method will be called after the partition re-assignment completes and before the
                 * consumer starts fetching data, and only as the result of a {@link Consumer#poll(java.time.Duration) poll(long)} call.
                 * @param partitions
                 */
                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    for (TopicPartition partition : partitions) {
//                        consumer.seek(partition, getOffsetFromDB());
                    }
                }
            });

            while (isRunning.get()) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord record : consumerRecords) {
                    // business logic with record
                    // ...

                    // update committed offset manually
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));  // committed offset = lastReadingOffset + 1
                }
                // use async to achieve higher performance
                consumer.commitAsync(currentOffsets, null);
            }
        } catch (Exception e) {
            LOG.error("error in consuming messages ", e);
        }
    }
}
