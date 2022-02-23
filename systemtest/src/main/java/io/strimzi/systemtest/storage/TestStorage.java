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

import static io.strimzi.operator.common.Util.sha1Prefix;
import static io.strimzi.systemtest.Constants.INFRA_NAMESPACE;

/**
 * TestStorage generate and stores values in the specific @see{ExtensionContext}. This ensures that if one want to
 * retrieve data from TestStorage it can be done via ExtensionContext (with help of @ConcurrentHashMap)
 */
final public class TestStorage {

    private static final String PRODUCER = "hello-world-producer";
    private static final String CONSUMER = "hello-world-consumer";
    private static final String CLUSTER_NAME_PREFIX = "my-cluster-";
    private static final Random RANDOM = new Random();

    private ExtensionContext extensionContext;
    private String namespaceName;
    private String clusterName;
    private String topicName;
    private String streamsTopicTargetName;
    private String kafkaClientsName;
    private String producerName;
    private String consumerName;
    private LabelSelector kafkaSelector;
    private LabelSelector zkSelector;

    public TestStorage(ExtensionContext extensionContext) {
        this(extensionContext, INFRA_NAMESPACE);
    }

    public TestStorage(ExtensionContext extensionContext, String namespaceName) {
        this.extensionContext = extensionContext;
        this.namespaceName = StUtils.isParallelNamespaceTest(extensionContext) ? StUtils.getNamespaceBasedOnRbac(namespaceName, extensionContext) : namespaceName;
        this.clusterName = CLUSTER_NAME_PREFIX + sha1Prefix(String.valueOf(RANDOM.nextInt(Integer.MAX_VALUE)));
        this.topicName = KafkaTopicUtils.generateRandomNameOfTopic();
        this.streamsTopicTargetName = KafkaTopicUtils.generateRandomNameOfTopic();
        this.kafkaClientsName = clusterName + "-" + Constants.KAFKA_CLIENTS;
        this.producerName = clusterName + "-" + PRODUCER;
        this.consumerName = clusterName  + "-" + CONSUMER;
        this.kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));
        this.zkSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.zookeeperStatefulSetName(clusterName));

        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.NAMESPACE_KEY, this.namespaceName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.CLUSTER_KEY, this.clusterName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.TOPIC_KEY, this.topicName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.STREAM_TOPIC_KEY, this.streamsTopicTargetName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.KAFKA_CLIENTS_KEY, this.kafkaClientsName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.PRODUCER_KEY, this.producerName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.CONSUMER_KEY, this.consumerName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.KAFKA_SELECTOR, this.kafkaSelector);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.ZOOKEEPER_SELECTOR, this.zkSelector);
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

    public String getTopicName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString();
    }

    public String getKafkaClientsName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_KEY).toString();
    }

    public String getProducerName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.PRODUCER_KEY).toString();
    }

    public String getConsumerName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CONSUMER_KEY).toString();
    }

    public LabelSelector getKafkaSelector() {
        return (LabelSelector) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_SELECTOR);
    }

    public LabelSelector getZookeeperSelector() {
        return (LabelSelector) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.ZOOKEEPER_SELECTOR);
    }
}
