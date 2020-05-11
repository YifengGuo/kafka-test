package com.yifeng.kafka_test.comsumer.custom.partition.assignor;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by guoyifeng on 5/11/20
 * <p>
 * commonly a partition of a topic will only be assigned to only one consumer in a consumer group
 * however we can break this law by overriding assign() of AbstractPartitionAssignor
 * this class is to show an example of an assignor which make partitions have broadcast feature
 */
public class BroadcastAssignor extends AbstractPartitionAssignor {
    /**
     * topic -> [consumer1, consumer2...]
     *
     * @param consumerMetadata
     * @return
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

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        Map<String, List<String>> consumersPerTopic = consumersPerTopic(subscriptions);
        // initial assignment with empty partition list
        subscriptions.keySet().forEach(consumerId -> assignment.put(consumerId, new ArrayList<>()));

        for (Map.Entry<String, List<String>> topicEntry : consumersPerTopic.entrySet()) {
            String topic = topicEntry.getKey();
            List<String> consumersForTopic = topicEntry.getValue();

            int partitionsNum = partitionsPerTopic.get(topic);
            if (partitionsNum == 0 || consumersForTopic.size() == 0) {
                continue;
            }

            List<TopicPartition> partitions = AbstractPartitionAssignor.partitions(topic, partitionsNum); // partitions of current topic
            // make each consumer which subscribed this topic be assigned with every partition
            if (!partitions.isEmpty()) {
                consumersForTopic.forEach(consumerId -> assignment.get(consumerId).addAll(partitions));
            }
        }
        return assignment;
    }

    @Override
    public String name() {
        return "broadcast";
    }
}
