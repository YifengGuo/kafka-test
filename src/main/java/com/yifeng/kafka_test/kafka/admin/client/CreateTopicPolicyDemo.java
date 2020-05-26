package com.yifeng.kafka_test.kafka.admin.client;

import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.CreateTopicPolicy;

import java.util.Map;

/**
 * Created by guoyifeng on 5/26/20
 *
 * implemented a customized policy used when validating new created topic on current Kafka cluster
 * add parameter on config/server.properties:
 *      create.topic.policy.class.name=org.apache.kafka.server.policy.CreateTopicPolicyDemo
 * then restart kafka
 *
 * in this demo policy, a valid topic shall at least have 5 partitions and each partition shall have at least have 2 replicas
 */
public class CreateTopicPolicyDemo implements CreateTopicPolicy {

    @Override
    public void validate(RequestMetadata requestMetadata) throws PolicyViolationException {
        if (requestMetadata.numPartitions() != null && requestMetadata.replicationFactor() != null) {
            if (requestMetadata.numPartitions() < 5) {
                throw new PolicyViolationException("A valid topic shall at least have 5 partitions, received "
                        + requestMetadata.numPartitions());
            }
            if (requestMetadata.replicationFactor() <= 1) {
                throw new PolicyViolationException("Each partition shall at least have 2 replicas, received "
                        + requestMetadata.replicationFactor());
            }
        }
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
