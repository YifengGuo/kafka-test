package com.yifeng.kafka_test.consumer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yifeng.kafka_test.comsumer.KafkaConsumerGenerator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by guoyifeng on 11/15/18
 */
public class KafkaConsumerTest {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerTest.class);
    private KafkaConsumer<String, String> consumer;
    private Map<Object, Integer> custCountryMap = new HashMap<>();
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
}
