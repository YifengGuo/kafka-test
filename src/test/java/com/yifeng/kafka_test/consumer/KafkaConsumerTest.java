package com.yifeng.kafka_test.consumer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yifeng.kafka_test.comsumer.KafkaConsumerGenerator;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by guoyifeng on 11/15/18
 */
@SuppressWarnings("Duplicates")
public class KafkaConsumerTest {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerTest.class);
    private KafkaConsumer<String, String> consumer;
    private Map<Object, Integer> custCountryMap = new HashMap<>();
    private Map<TopicPartition, OffsetAndMetadata> currentOffsets;
    @Before
    public void initial() {
        consumer = KafkaConsumerGenerator.createConsumer();
        consumer.subscribe(Collections.singletonList("CustomerCountry"));
    }

    @Test
    public void testSimplePoll() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
                for (ConsumerRecord record : records) {
                    LOG.info("topic = {}," +
                            " partition = {}," +
                            " offset = {}," +
                            " customer = {}," +
                            " country = {}", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    int updateCount = 1;
                    if (custCountryMap.containsValue(record.value())) {
                        updateCount = custCountryMap.get(record.value()) + 1;
                    }
                    custCountryMap.put(record.value(), updateCount);
                    String json = JSON.toJSONString(custCountryMap);
                    JSONObject res = JSONObject.parseObject(json);
                    LOG.info(res.toString());
                }
            }
        } finally {
            consumer.close();
        }
    }

    @Test
    public void testCommitSync() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
                for (ConsumerRecord record : records) {
                    LOG.info("topic = {}," +
                            " partition = {}," +
                            " offset = {}," +
                            " customer = {}," +
                            " country = {}", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    int updateCount = 1;
                    if (custCountryMap.containsValue(record.value())) {
                        updateCount = custCountryMap.get(record.value()) + 1;
                    }
                    custCountryMap.put(record.value(), updateCount);
                    String json = JSON.toJSONString(custCountryMap);
                    JSONObject res = JSONObject.parseObject(json);
                    LOG.info(res.toString());
                }
                try {
                    consumer.commitSync();
                } catch (CommitFailedException e) {
                    LOG.error("consumer failed to commit {}", e);
                }
            }
        } finally {
            consumer.close();
        }
    }

    @Test
    public void testCommitAsync() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
                for (ConsumerRecord record : records) {
                    LOG.info("topic = {}," +
                            " partition = {}," +
                            " offset = {}," +
                            " customer = {}," +
                            " country = {}", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    int updateCount = 1;
                    if (custCountryMap.containsValue(record.value())) {
                        updateCount = custCountryMap.get(record.value()) + 1;
                    }
                    custCountryMap.put(record.value(), updateCount);
                    String json = JSON.toJSONString(custCountryMap);
                    JSONObject res = JSONObject.parseObject(json);
                    LOG.info(res.toString());
                }
                consumer.commitAsync();
            }
        } finally {
            consumer.close();
        }
    }

    @Test
    // commit asynchronously with callback()
    public void testCommitAsyncWithCallback() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
                for (ConsumerRecord record : records) {
                    LOG.info("topic = {}," +
                            " partition = {}," +
                            " offset = {}," +
                            " customer = {}," +
                            " country = {}", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    int updateCount = 1;
                    if (custCountryMap.containsValue(record.value())) {
                        updateCount = custCountryMap.get(record.value()) + 1;
                    }
                    custCountryMap.put(record.value(), updateCount);
                    String json = JSON.toJSONString(custCountryMap);
                    JSONObject res = JSONObject.parseObject(json);
                    LOG.info(res.toString());
                }
                consumer.commitAsync((offsets, exception) -> {
                    if (exception != null) {
                        LOG.error("error in committing asynchronously, offsets are {}, excxption is {}", offsets, exception);
                    }
                });
            }
        } finally {
            consumer.close();
        }
    }

    @Test
    // used when we know this is the last commit before closing the consumer or before rebalance
    // to make sure there is no error in committing
    public void testCommitSyncAndAsync() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
                for (ConsumerRecord record : records) {
                    LOG.info("topic = {}," +
                            " partition = {}," +
                            " offset = {}," +
                            " customer = {}," +
                            " country = {}", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    int updateCount = 1;
                    if (custCountryMap.containsValue(record.value())) {
                        updateCount = custCountryMap.get(record.value()) + 1;
                    }
                    custCountryMap.put(record.value(), updateCount);
                    String json = JSON.toJSONString(custCountryMap);
                    JSONObject res = JSONObject.parseObject(json);
                    LOG.info(res.toString());
                }
                consumer.commitAsync();  // if everything is fine, async is faster than sync
            }
        } catch (Exception e) {
            LOG.error("error in committing, {}", e);
        } finally {
            try {
                consumer.commitSync();  // before closing, there is not next commit, and if there is error,
                                        // commit with sync will retry until commit done or meet unrecoverable failure
            } catch (CommitFailedException e) {
                LOG.error("error in committing synchronously, {}", e);
            }
        }
    }

    @Test
    // when batch is too large or we just want to commit more frequently before consuming all records
    // pass a Map of partitions and offsets we wish to commit
    public void testCommitPartialOffsets() {
        currentOffsets = new HashMap<>();
        int count = 0;
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
                for (ConsumerRecord record : records) {
                    LOG.info("topic = {}," +
                            " partition = {}," +
                            " offset = {}," +
                            " customer = {}," +
                            " country = {}", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, "no metadata"));
                    if (count % 1000 == 0) {
                        consumer.commitAsync(currentOffsets, null); // commit whenever consuming 1000 records
                    }
                    count++;
                }
            }
        } finally {
            consumer.close();
        }
    }

    @Test
    public void testRebanlaceHandler() {
        try {
            consumer.subscribe(Collections.singletonList("CustomerCountry"), new RebalanceHandler()); // pass rebalancehandler in subscribe()
                                                                                                      // so it will be invoked by the cnsumer
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
                for (ConsumerRecord record : records) {
                    LOG.info("topic = {}," +
                            " partition = {}," +
                            " offset = {}," +
                            " customer = {}," +
                            " country = {}", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, "no metadata"));
                }
                consumer.commitAsync(currentOffsets, null);
            }
        } catch (Exception e) {
            LOG.error("error in consuming, {}", e);
        } finally {
            try {
                consumer.commitSync();
            } catch (CommitFailedException e) {
                LOG.error("error in committing synchronously, {}", e);
            }
        }
    }

    private class RebalanceHandler implements ConsumerRebalanceListener {
        @Override
        // consumer stop consuming messages -> onPartitionsRevoked() -> rebalance starts
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            currentOffsets = new HashMap<>();
            LOG.info("Lost partitions in rebalance. Committing current offsets: " + currentOffsets);
            consumer.commitSync(currentOffsets);
        }

        @Override
        // partitions have been reassigned to the broker -> onPartitionsAssigned() -> new consumer starts to consume
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            // pass
        }
    }
}
