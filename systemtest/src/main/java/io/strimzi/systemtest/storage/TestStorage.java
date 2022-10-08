/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.storage;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Random;

import static io.strimzi.operator.common.Util.hashStub;
import static io.strimzi.systemtest.Constants.INFRA_NAMESPACE;
import static io.strimzi.systemtest.Constants.MESSAGE_COUNT;

/**
 * TestStorage generate and stores values in the specific @see{ExtensionContext}. This ensures that if one want to
 * retrieve data from TestStorage it can be done via ExtensionContext (with help of @ConcurrentHashMap)
 */
final public class TestStorage {

    private static final String PRODUCER = "hello-world-producer";
    private static final String CONSUMER = "hello-world-consumer";
    private static final String ADMIN = "admin-client";
    private static final String USER = "user";
    private static final String CLUSTER_NAME_PREFIX = "my-cluster-";
    private static final Random RANDOM = new Random();

    private ExtensionContext extensionContext;
    private String namespaceName;
    private String clusterName;
    private String targetClusterName;
    private String topicName;
    private String targetTopicName;
    private String streamsTopicTargetName;
    private String scraperName;
    private String producerName;
    private String consumerName;
    private String adminName;
    private String userName;
    private String eoDeploymentName;
    private String kafkaStatefulSetName;
    private String zkStatefulSetName;
    private LabelSelector kafkaSelector;
    private LabelSelector zkSelector;
    private int messageCount;

    public TestStorage(ExtensionContext extensionContext) {
        this(extensionContext, INFRA_NAMESPACE);
    }

    public TestStorage(ExtensionContext extensionContext, String namespaceName) {
        this(extensionContext, namespaceName, MESSAGE_COUNT);
    }

    public TestStorage(ExtensionContext extensionContext, String namespaceName, int messageCount) {
        this.extensionContext = extensionContext;
        this.namespaceName = StUtils.isParallelNamespaceTest(extensionContext) ? StUtils.getNamespaceBasedOnRbac(namespaceName, extensionContext) : namespaceName;
        this.clusterName = CLUSTER_NAME_PREFIX + hashStub(String.valueOf(RANDOM.nextInt(Integer.MAX_VALUE)));
        this.targetClusterName = CLUSTER_NAME_PREFIX + hashStub(String.valueOf(RANDOM.nextInt(Integer.MAX_VALUE))) + "-target";
        this.topicName = KafkaTopicUtils.generateRandomNameOfTopic();
        this.targetTopicName = topicName + "-target";
        this.streamsTopicTargetName = KafkaTopicUtils.generateRandomNameOfTopic();
        this.scraperName = clusterName + "-" + Constants.SCRAPER_NAME;
        this.producerName = clusterName + "-" + PRODUCER;
        this.consumerName = clusterName  + "-" + CONSUMER;
        this.adminName = clusterName + "-" + ADMIN;
        this.userName = clusterName + "-" + USER;
        this.eoDeploymentName = KafkaResources.entityOperatorDeploymentName(clusterName);
        this.kafkaStatefulSetName = KafkaResources.kafkaStatefulSetName(clusterName);
        this.zkStatefulSetName = KafkaResources.zookeeperStatefulSetName(clusterName);
        this.kafkaSelector = KafkaResource.getLabelSelector(clusterName, this.kafkaStatefulSetName);
        this.zkSelector = KafkaResource.getLabelSelector(clusterName, this.zkStatefulSetName);
        this.messageCount = messageCount;

        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.NAMESPACE_KEY, this.namespaceName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.CLUSTER_KEY, this.clusterName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.TARGET_CLUSTER_KEY, this.targetClusterName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.TOPIC_KEY, this.topicName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.TARGET_TOPIC_KEY, this.targetTopicName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.STREAM_TOPIC_KEY, this.streamsTopicTargetName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.SCRAPER_KEY, this.scraperName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.PRODUCER_KEY, this.producerName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.CONSUMER_KEY, this.consumerName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.ADMIN_KEY, this.adminName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.USER_NAME_KEY, this.userName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.ENTITY_OPERATOR_NAME_KEY, this.eoDeploymentName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.KAFKA_STATEFULSET_NAME_KEY, this.kafkaStatefulSetName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.ZOOKEEPER_STATEFULSET_NAME_KEY, this.zkStatefulSetName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.KAFKA_SELECTOR, this.kafkaSelector);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.ZOOKEEPER_SELECTOR, this.zkSelector);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.MESSAGE_COUNT_KEY, this.messageCount);
    }

    public void addToTestStorage(String key, Object value) {
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(key, value);
    }

    public Object retrieveFromTestStorage(String key) {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(key);
    }

    public String getNamespaceName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.NAMESPACE_KEY).toString();
    }

    public String getClusterName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString();
    }

    public String getTargetClusterName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TARGET_CLUSTER_KEY).toString();
    }

    public String getTopicName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString();
    }

    public String getTargetTopicName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TARGET_TOPIC_KEY).toString();
    }

    public String getScraperName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.SCRAPER_KEY).toString();
    }

    public String getProducerName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.PRODUCER_KEY).toString();
    }

    public String getConsumerName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CONSUMER_KEY).toString();
    }

    public String getAdminName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.ADMIN_KEY).toString();
    }

    public String getUserName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.USER_NAME_KEY).toString();
    }

    public String getEoDeploymentName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.ENTITY_OPERATOR_NAME_KEY).toString();
    }

    public String getKafkaStatefulSetName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_STATEFULSET_NAME_KEY).toString();
    }
    public String getZookeeperStatefulSetName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.ZOOKEEPER_STATEFULSET_NAME_KEY).toString();
    }

    public LabelSelector getKafkaSelector() {
        return (LabelSelector) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_SELECTOR);
    }

    public LabelSelector getZookeeperSelector() {
        return (LabelSelector) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.ZOOKEEPER_SELECTOR);
    }

    public int getMessageCount() {
        return (int) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.MESSAGE_COUNT_KEY);
    }

}
