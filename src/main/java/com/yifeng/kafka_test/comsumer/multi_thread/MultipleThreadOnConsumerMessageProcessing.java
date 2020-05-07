package com.yifeng.kafka_test.comsumer.multi_thread;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by guoyifeng on 5/6/20
 */
public class MultipleThreadOnConsumerMessageProcessing {

    private static final Logger LOG = LoggerFactory.getLogger(MultipleThreadOnConsumerMessageProcessing.class);

    private static final String BROKER_LIST = "localhost:9092";

    private static final String TOPIC = "demo-topic";

    private static final String GROUP_ID = "demo-group";

    // to track the source of requests beyond just ip and port by allowing a logical application name to be included
    // in Kafka logs and monitoring aggregates
    // if not set, default value would be like consumer-1, consumer-2...
    private static final String CLIENT_ID = "demo-client-id";

    private static final AtomicBoolean isRunning = new AtomicBoolean(true);

    private static Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

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
        KafkaConsumerThread consumerThread =
                new KafkaConsumerThread(properties, Collections.singletonList(TOPIC),
                        Runtime.getRuntime().availableProcessors());
        // to achieve higher parallel performance, one could instantiate more KafkaConsumerThread
        consumerThread.start();
    }

    /**
     * Each kafka consumer thread holds a thread pool which is to submit runnable task for RecordsHandler
     * RecordsHandler is responsible for handling time-consuming message processing task
     */
    private static class KafkaConsumerThread extends Thread {
        private KafkaConsumer<String, String> kafkaConsumer;
        private ExecutorService executorService;
        private int threadNum; // number of threads in thread pool

        public KafkaConsumerThread(Properties properties, Collection<String> topics, int threadNum) {
            this.kafkaConsumer = new KafkaConsumer<>(properties);
            this.kafkaConsumer.subscribe(topics);
            // init thread pool
            executorService = new ThreadPoolExecutor(threadNum, threadNum, 0L, TimeUnit.SECONDS,
                    new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
            this.threadNum = threadNum;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1L));
                    if (!records.isEmpty()) {
                        this.executorService.submit(new RecordsHandler(records));
                    }
                    // after submit currently fetched records, old offsets shall be committed and cleared
                    // however this way still has chance to lose data
                    // assume thread-1 is consuming 0-99, thread-2 is consuming 100-199
                    // suppose thread-2 completes consuming 100-199 and commit offset to 200 but
                    // thread-1 failed and throw some exception, then 0-99 messages are lost
                    // one idea to solve this is to cache fetched records into a fixed size sliding window (could be a deque)
                    // this sliding window contains fixed number of ConsumerRecords which equals num of worker threads
                    // this sliding window has a startOffset and an endOffset. Whenever records pointed by startOffset
                    // is consumed, remove that ConsumerRecords from window, commit startOffset, move startOffset to next batch
                    // and fetch new one from Kafka and put into tail of window and move endOffset to that records
                    // external logic will only commit offset when worker threads complete consuming current startOffset batch

                    // number of ConsumerRecords in window decides parallelism
                    // give startOffset a timeout, if exceeds, try to re-consume locally
                    // if retry failed, move that batch to re-try queue
                    // if re-try queue still has failure, move to dead queue which means this kind of msgs cannot be consumed
                    // usually due to local business logic
                    synchronized (offsets) {
                        if (!offsets.isEmpty()) {
                            kafkaConsumer.commitSync(offsets);
                            offsets.clear();
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("error in fetching records from Kafka and send by thread pool", e);
            } finally {
                kafkaConsumer.close();
            }
        }
    }

    private static class RecordsHandler implements Runnable {

        private ConsumerRecords<String, String> consumerRecords;

        public RecordsHandler(ConsumerRecords<String, String> consumerRecords) {
            this.consumerRecords = consumerRecords;
        }

        @Override
        public void run() {
            for (TopicPartition tp : consumerRecords.partitions()) {
                List<ConsumerRecord<String, String>> tpRecords = consumerRecords.records(tp);
                // process messages here which could be time-consuming
                // could be ETL
                // could be insert to external DB

                // check and update offset
                long lastConsumedOffset = tpRecords.get(tpRecords.size() - 1).offset();
                synchronized (offsets) {  // lock offsets so that other thread cannot access it concurrently
                    if (!offsets.containsKey(tp)) {
                        offsets.put(tp, new OffsetAndMetadata(lastConsumedOffset + 1)); // next time poll pos (last committed pos) = lastConsumedPos + 1
                    } else {
                        long existedPos = offsets.get(tp).offset();
                        if (existedPos < lastConsumedOffset + 1) { // currently existed offset position can be safely removed
                            offsets.put(tp, new OffsetAndMetadata(lastConsumedOffset + 1));
                        }
                    }
                }
            }
            // simple and intuitive way
//            for (ConsumerRecord<String, String> record : consumerRecords) {
//                // process messages here which could be time-consuming
//                // could be ETL
//                // could be insert to external DB
//            }
        }
    }
}
