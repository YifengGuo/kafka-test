package com.yifeng.kafka_test.comsumer.custom.partition.assignor;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by guoyifeng on 5/9/20
 *
 * randomly assign a partition of topic to consumer which has subscribed this topic
 * to use this, declare in consumer properties initialization:
 * properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RandomAssignor.class.getName());
 */
public class RandomAssignor extends AbstractPartitionAssignor {
    public RandomAssignor() {
        super();
    }

    /**
     * get topic to consumer which subscribed this topic mappings
     * @param consumerMetadata
     * @return topic -> [consumer1, consumer2 ...]
     */
    private Map<String, List<String>> consumersPerTopic(Map<String, Subscription> consumerMetadata) {
        Map<String, List<String>> res = new HashMap<>();
        for (Map.Entry<String, Subscription> subscriptionEntry : consumerMetadata.entrySet()) {
            String consumerId = subscriptionEntry.getKey();
            for (String topic : subscriptionEntry.getValue().topics()) {
                put(res, topic, consumerId);
            }
        }
        return res;
    }

    /**
     * logic for assigning partitions for consumer in group
     * @param partitionsPerTopic
     * @param subscriptions
     * @return
     */
    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic, Map<String, Subscription> subscriptions) {
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        Map<String, List<String>> consumersPerTopic = consumersPerTopic(subscriptions);
        // initial assignment with empty partition list
        for (String consumerId : consumersPerTopic.keySet()) {
            assignment.put(consumerId, new ArrayList<>());
        }

        // distribute partition to random consumer which has subscribed its topic
        for (Map.Entry<String, List<String>> topicEntry : consumersPerTopic.entrySet()) {
            String topic = topicEntry.getKey();
            List<String> consumersForTopic = topicEntry.getValue(); // consumers which have subscribed this topic
            int consumerNum = consumersForTopic.size();

            // sanity check
            int partitionsNum = partitionsPerTopic.get(topic);
            if (partitionsNum == 0) {
                continue; // means this topic has no partition
            }

            // get all partitions info of current topic
            List<TopicPartition> partitions = AbstractPartitionAssignor.partitions(topic, partitionsNum);
            // distribute partitions randomly to consumers
            for (TopicPartition partition : partitions) {
                int rand = ThreadLocalRandom.current().nextInt(consumerNum);
                String randConsumer = consumersForTopic.get(rand);
                assignment.get(randConsumer).add(partition);
            }
        }
        return assignment;
    }

    /**
     * each partition assignor has a name for future leader election or consumer participating consumer group
     * @return
     */
    @Override
    public String name() {
        return "random";
    }
}
