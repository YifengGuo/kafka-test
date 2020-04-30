package com.yifeng.kafka_test.interceptor.consumer;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by guoyifeng on 4/30/20
 *
 * this interceptor is to filter out messages whose create timestamp till now is treated as expired
 */
public class ConsumerTTLInterceptor implements ConsumerInterceptor<String, String> {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerTTLInterceptor.class);

    private static final long EXPIRE_INTERVAL = 1000 * 10L;  // 10s

    /**
     * This is called just BEFORE the records are returned by poll()
     * so we can modified polled records here by applying filtering, value modification etc.
     * @param records
     * @return
     */
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        Map<TopicPartition, List<ConsumerRecord<String, String>>> notExpiredTotalRecords = new HashMap<>();
        Set<TopicPartition> partitions = records.partitions();
        long now = System.currentTimeMillis();
        for (TopicPartition partition : partitions) {
            List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
            List<ConsumerRecord<String, String>> notExpiredPartitionRecords = new ArrayList<>();
            for (ConsumerRecord<String, String> record : partitionRecords) {
                if (now - record.timestamp() < EXPIRE_INTERVAL) {
                    notExpiredPartitionRecords.add(record);
                }
            }
            if (!notExpiredPartitionRecords.isEmpty()) {
                notExpiredTotalRecords.put(partition, notExpiredPartitionRecords);
            }
        }
        return new ConsumerRecords<>(notExpiredTotalRecords);
    }

    /**
     * This is called when commit request returns successfully from the broker. (AFTER a successful commit)
     * usually used for tracing offsets info especially when using commitAsync() we have no idea about offsets
     * but we can get offset info here
     * @param offsets
     */
    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        offsets.forEach((tp, offset) -> LOG.info("partition {} : offset {}", tp, offset));
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
