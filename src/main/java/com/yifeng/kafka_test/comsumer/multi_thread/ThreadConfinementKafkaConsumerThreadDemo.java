package com.yifeng.kafka_test.comsumer.multi_thread;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by guoyifeng on 5/6/20
 *
 * use thread confinement way which is to instantiate independent kakfa consumer in each thread to use in multiple thread
 * env
 *
 * Pros: Each consumer thread can consume messages in order from assigned partitions
 * Cons: Each thread of consumer will create a TCP connection, so a huge number of connections will cost many system resources
 */
public class ThreadConfinementKafkaConsumerThreadDemo {

    private static final Logger LOG = LoggerFactory.getLogger(ThreadConfinementKafkaConsumerThreadDemo.class);

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
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);  // enable auto commit for multiple threads manual commit is complex
        return properties;
    }


    public static void main(String[] args) {
        Properties properties = initConfig();
        int consumerThreadNum = 4;
        for (int i = 0; i < consumerThreadNum; ++i) {
            new KafkaConsumerThread(properties, Arrays.asList(TOPIC)).start();
        }
    }

    private static class KafkaConsumerThread extends Thread {
        private KafkaConsumer<String, String> consumer;

        public KafkaConsumerThread(Properties props, Collection<String> topics) {
            this.consumer = new KafkaConsumer<>(props);
            this.consumer.subscribe(topics);
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> consumerRecords = this.consumer.poll(Duration.ofSeconds(1L));
                    for (ConsumerRecord<String, String> record : consumerRecords) {
                        // business logic to process each record
                        // usually bottleneck is here for processing messages because poll() is very quick normally
                        // so another way is to apply multiple threads to process fetched messages here
                    }
                }
            } catch (Exception e) {
                LOG.error("error in getting records from kafka by current thread {}", currentThread().getId());
            } finally {
                consumer.close();
            }
        }
    }
}
