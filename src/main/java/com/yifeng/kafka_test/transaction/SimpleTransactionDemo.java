package com.yifeng.kafka_test.transaction;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by guoyifeng on 5/20/20
 */
public class SimpleTransactionDemo {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleTransactionDemo.class);

    private static String brokerList = "localhost:9092";

    private static String transactionalId = "my-transactional-id";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        producer.initTransactions();
        producer.beginTransaction();

        try {
            ProducerRecord<String, String> record1 = new ProducerRecord<>("topic", "key1", "msg1");
            producer.send(record1);
            ProducerRecord<String, String> record2 = new ProducerRecord<>("topic", "key2", "msg2");
            producer.send(record2);
            ProducerRecord<String, String> record3 = new ProducerRecord<>("topic", "key3", "msg3");
            producer.send(record3);

            producer.commitTransaction();
        } catch (Exception e) {
            LOG.error("error in sending msgs in transaction ", e);
            producer.abortTransaction();
        } finally {
            producer.close();
        }
    }
}
