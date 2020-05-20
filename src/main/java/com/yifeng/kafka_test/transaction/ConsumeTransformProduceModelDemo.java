package com.yifeng.kafka_test.transaction;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by guoyifeng on 5/20/20
 *
 * A demo for classical consume-transform-produce model:
 *      topic-source -> KafkaConsumer -> Application (transform) -> KafkaProducer -> topic-sink
 */
public class ConsumeTransformProduceModelDemo {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumeTransformProduceModelDemo.class);

    private static String brokerList = "localhost:9092";

    private static String transactionalId = "my-transactional-id";

    private static String groupId = "my-group";

    private static String sourceTopic = "topic-source";

    private static String sinkTopic = "topic-sink";

    private static Properties getConsumerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        return properties;
    }

    private static Properties getProducerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // to enable transaction and idempotence, auto commit must be disabled
        return properties;
    }

    public static void main(String[] args) {
        // consumer initialization
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getConsumerProperties());
        consumer.subscribe(Collections.singletonList(sourceTopic));

        // producer and transaction initialization
        KafkaProducer<String, String> producer = new KafkaProducer<>(getProducerProperties());
        producer.initTransactions();

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                if (!consumerRecords.isEmpty()) {
                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    producer.beginTransaction(); // open transaction
                    Set<TopicPartition> partitions = consumerRecords.partitions();
                    for (TopicPartition partition : partitions) {
                        List<ConsumerRecord<String, String>> partitionRecords = consumerRecords.records(partition);
                        for (ConsumerRecord<String, String> message : partitionRecords) {
                            // business logic or ETL
                            TimeUnit.MILLISECONDS.sleep(10);
                            ProducerRecord<String, String> modifiedMessage = new ProducerRecord<>(sinkTopic,
                                    message.key(), message.value());
                            producer.send(modifiedMessage); // transform-sink
                        }
                        // update partition offset when current partition consumption is done
                        long lastConsumedOffset = partitionRecords.get(partitionRecords.size() -1).offset();
                        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(lastConsumedOffset + 1);
                        offsets.put(partition, offsetAndMetadata);
                    }
                    // update offset and commit transaction after handling each partition in current poll()
                    producer.sendOffsetsToTransaction(offsets, groupId);
                    producer.commitTransaction();
                }
            }
        } catch (Exception e) {
            producer.abortTransaction();
            LOG.error("error in consume-transform-produce, abort current transaction", e);
        } finally {
            producer.close();
            consumer.close();
        }
    }
}
