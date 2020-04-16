package com.yifeng.kafka_test.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by guoyifeng on 4/16/20
 */
public class CustomPartitioner implements Partitioner {

    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        if (key == null) {
            // if key is null, use round-robin to route msg to topic
            return counter.getAndIncrement() % partitions.size();
        } else {
            // invoke MurmurHash2 to calculate hash and route msg to topic
            // MurmurHash2 has high performance and low collision possibility
            return Utils.toPositive(Utils.murmur2(keyBytes)) % partitions.size();
        }
        // to enable this partitioner:
        // props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName()) ;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
