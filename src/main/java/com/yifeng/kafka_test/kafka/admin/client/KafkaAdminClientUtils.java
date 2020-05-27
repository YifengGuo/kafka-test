package com.yifeng.kafka_test.kafka.admin.client;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Created by guoyifeng on 5/26/20
 */
public class KafkaAdminClientUtils {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaAdminClientUtils.class);

    private static final String REQUEST_TIMEOUT_MS_CONFIG = "10000";

    private String brokerList;

    public KafkaAdminClientUtils(String brokerList) {
        this.brokerList = brokerList;
    }

    public void createNewTopic(String topicName, int partitionNum, int replicationFactor) {
        try (AdminClient adminClient = initAdminClient()) {
            NewTopic newTopic = new NewTopic(topicName, partitionNum, (short) replicationFactor);
            // if need to customize some configuration on new created topics
            Map<String, String> configs = new HashMap<>();
            configs.put("cleanup.policy", "compact");
            newTopic.configs(configs);
            CreateTopicsResult result = adminClient.createTopics(Collections.singletonList(newTopic));
            result.all().get();
            LOG.info("topic {} created", topicName);
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("error in creating new topic {} ", topicName, e);
        }
    }

    public void deleteTopics(List<String> topics) {
        try (AdminClient adminClient = initAdminClient()) {
            DeleteTopicsResult result = adminClient.deleteTopics(topics);
            result.all().get();
            LOG.info("topics {} deleted", topics);
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("error in deleting topics {}", topics, e);
        }
    }

    public Set<String> listTopics() {
        Set<String> topics = new HashSet<>();
        try (AdminClient adminClient = initAdminClient()) {
            ListTopicsResult result = adminClient.listTopics();
             topics = result.names().get();
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("error in listing topics", e);
        }
        return topics;
    }

    public Map<String, TopicDescription> describeTopics(List<String> topics) {
        Map<String, TopicDescription> map = new HashMap<>();
        try (AdminClient adminClient = initAdminClient()) {
            DescribeTopicsResult result = adminClient.describeTopics(topics);
            map = result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("error in describing topics {}", topics, e);
        }
        return map;
    }

    public TopicDescription describeTopic(String topic) {
        try (AdminClient adminClient = initAdminClient()) {
            DescribeTopicsResult result = adminClient.describeTopics(Collections.singletonList(topic));
            TopicDescription description = result.all().get().get(topic);
            return description;
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("error in getting description on topic {}", topic, e);
        }
        return null;
    }

    /****** describe configs and alter configs  ******/

    public Config describeConfigs(ConfigResource.Type configType, String name) {
        try (AdminClient adminClient = initAdminClient()) {
            ConfigResource configResource = new ConfigResource(configType, name);
            DescribeConfigsResult result = adminClient.describeConfigs(Collections.singletonList(configResource));
            Config config = result.all().get().get(configResource);
            LOG.info("description of type: {} name: {} is {}", configType, name, config);
            return config;
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("error in getting description on type: {} name: {}", configType, name, e);
        }
        return null;
    }

    /**
     * example: "cleanup.policy": "compact"
     */
    public void alterConfigs(ConfigResource.Type configType, String name, String configName, String configValue) {
        try (AdminClient adminClient = initAdminClient()) {
            ConfigResource configResource = new ConfigResource(configType, name);
            ConfigEntry configEntry = new ConfigEntry(configName, configValue);
            Config config = new Config(Collections.singletonList(configEntry));
            Map<ConfigResource, Config> configMap = new HashMap<>();
            configMap.put(configResource, config);
            AlterConfigsResult result = adminClient.alterConfigs(configMap);
            result.all().get();
            LOG.info("alter configs on type: {} name: {}", configType, name);
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("error in altering configs type: {} name: {}", configType, name);
        }
    }

    /*****    increase number of partitions of topic ******/

    public void increasePartitions(String topic, int newPartitionNum) {
        try (AdminClient adminClient = initAdminClient()) {
            NewPartitions newPartitions = NewPartitions.increaseTo(newPartitionNum);
            Map<String, NewPartitions> map = new HashMap<>();
            map.put(topic, newPartitions);
            CreatePartitionsResult result = adminClient.createPartitions(map);
            result.all().get();
            LOG.info("increasing number of partitions to {} for topic {} ", newPartitionNum, topic);
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("error in increasing number of partitions to {} for topic {} ", newPartitionNum, topic);
        }
    }

    private AdminClient initAdminClient() {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_MS_CONFIG);  // by default 10s timeout
        return AdminClient.create(props);
    }
}
