package com.yifeng.kafka_test.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

/**
 * Created by guoyifeng on 10/12/18
 */
public class KafkaProducerGeneratorTest {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducerGeneratorTest.class);
    private KafkaProducer<String, String> producer;
    private ProducerRecord<String, String> record;
    @Before
    public void initial() {
        producer = KafkaProducerGenerator.createProducer();

        record = new ProducerRecord<String, String>(
                "CustomerCountry",
                "Precision Products",
                "France");
    }

    @Test
    public void testSimpleSender() {
        try {
            producer.send(record);
        } catch (Exception e) {
            LOG.info("error in sending record to consumer, {}", e.getMessage());
        }
    }

    @Test
    public void testSynchronousSender() {
        try {
            LOG.info("{}", producer.send(record).get());  // get() wait for a reply from kafka
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testAsynchronousSender() {
        // send record with a callback function
        // callback() will be triggered when it receives a response from the kafka broker
        producer.send(record, new DemoProducerCallback());
    }

    private class DemoProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                e.printStackTrace();
            }
        }
    }
}