package com.yifeng.kafka_test.comsumer.custom.partition.assignor;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by guoyifeng on 5/9/20
 */
public class RandomAssignor extends AbstractPartitionAssignor {
    public RandomAssignor() {
        super();
    }

    @Override
    public Subscription subscription(Set<String> topics) {
        return super.subscription(topics);
    }

    /**
     * logic for assigning partitions for consumer in group
     * @param metadata
     * @param subscriptions
     * @return
     */
    @Override
    public Map<String, Assignment> assign(Cluster metadata, Map<String, Subscription> subscriptions) {
        return super.assign(metadata, subscriptions);
    }

    @Override
    public void onAssignment(Assignment assignment) {
        super.onAssignment(assignment);
    }

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic, Map<String, Subscription> subscriptions) {
        return null;
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
