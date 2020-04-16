package com.yifeng.kafka_test.interceptor.producer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by guoyifeng on 4/16/20
 */
public class ProducerInterceptor1 implements ProducerInterceptor<String, String> {

    private volatile long sendSuccess = 0L;

    private volatile long sendFailure = 0L;

    private static final Logger LOG = LoggerFactory.getLogger(ProducerInterceptor1.class);

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String modifiedValue = "prefix1-" + record.value();
        return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), record.key(), record.value());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            sendFailure++;
        } else {
            sendSuccess++;
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public void close() {
        double successRatio = sendSuccess * 1.0 / (sendSuccess + sendFailure) * 100;
        LOG.info("Successful ratio of sending msgs to topic is {}%", successRatio);
    }
}
