/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.storage;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaTracingClients;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Random;

import static io.strimzi.operator.common.Util.hashStub;
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
    private String testName;
    private String namespaceName;
    private String clusterName;
    private String kafkaNodePoolName;
    private String sourceClusterName;
    private String targetClusterName;
    private String topicName;
    private String targetTopicName;
    private String mirroredSourceTopicName;
    private String streamsTopicTargetName;
    private String scraperName;
    private String producerName;
    private String consumerName;
    private String adminName;
    private String username;
    private String sourceUsername;
    private String targetUsername;
    private String kafkaUsername;
    private String eoDeploymentName;
    private String kafkaStatefulSetName;
    private String zkStatefulSetName;
    private LabelSelector kafkaSelector;
    private LabelSelector zkSelector;
    private LabelSelector kafkaConnectSelector;
    private LabelSelector mm2Selector;
    private int messageCount;
    private long testExecutionStartTime;


    public TestStorage(ExtensionContext extensionContext) {
        this(extensionContext, Environment.TEST_SUITE_NAMESPACE);
    }

    public TestStorage(ExtensionContext extensionContext, String namespaceName) {
        this(extensionContext, namespaceName, MESSAGE_COUNT);
    }

    public TestStorage(ExtensionContext extensionContext, String namespaceName, int messageCount) {

        this.extensionContext = extensionContext;
        this.testName = extensionContext.getTestMethod().isPresent() ? extensionContext.getTestMethod().get().getName() : "null-testname";
        this.namespaceName = StUtils.isParallelNamespaceTest(extensionContext) ? StUtils.getNamespaceBasedOnRbac(namespaceName, extensionContext) : namespaceName;
        this.clusterName = CLUSTER_NAME_PREFIX + hashStub(String.valueOf(RANDOM.nextInt(Integer.MAX_VALUE)));
        this.kafkaNodePoolName = Constants.KAFKA_NODE_POOL_PREFIX + hashStub(clusterName);
        this.sourceClusterName = clusterName + "-source";
        this.targetClusterName = clusterName + "-target";
        this.topicName = KafkaTopicUtils.generateRandomNameOfTopic();
        this.targetTopicName = topicName + "-target";
        this.mirroredSourceTopicName = sourceClusterName + "." + topicName;
        this.streamsTopicTargetName = KafkaTopicUtils.generateRandomNameOfTopic();
        this.scraperName = clusterName + "-" + Constants.SCRAPER_NAME;
        this.producerName = clusterName + "-" + PRODUCER;
        this.consumerName = clusterName  + "-" + CONSUMER;
        this.adminName = clusterName + "-" + ADMIN;
        this.username = clusterName + "-" + USER;
        this.sourceUsername = username + "-source";
        this.targetUsername = username + "-target";
        this.kafkaUsername = KafkaUserUtils.generateRandomNameOfKafkaUser();
        this.eoDeploymentName = KafkaResources.entityOperatorDeploymentName(clusterName);
        this.kafkaStatefulSetName = Environment.isKafkaNodePoolsEnabled() ?
            this.clusterName + "-" + this.kafkaNodePoolName : KafkaResources.kafkaStatefulSetName(clusterName);
        this.zkStatefulSetName = KafkaResources.zookeeperStatefulSetName(clusterName);
        this.kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));
        this.zkSelector = KafkaResource.getLabelSelector(clusterName, this.zkStatefulSetName);
        this.kafkaConnectSelector = KafkaConnectResource.getLabelSelector(clusterName, KafkaConnectResources.deploymentName(clusterName));
        this.mm2Selector = KafkaMirrorMaker2Resource.getLabelSelector(clusterName, KafkaMirrorMaker2Resources.deploymentName(clusterName));
        this.messageCount = messageCount;
        this.testExecutionStartTime = System.currentTimeMillis();

        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.TEST_NAME_KEY, this.testName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.NAMESPACE_KEY, this.namespaceName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.CLUSTER_KEY, this.clusterName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.KAFKA_NODE_POOL_KEY, this.kafkaNodePoolName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.SOURCE_CLUSTER_KEY, this.sourceClusterName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.TARGET_CLUSTER_KEY, this.targetClusterName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.TOPIC_KEY, this.topicName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.TARGET_TOPIC_KEY, this.targetTopicName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.MIRRORED_SOURCE_TOPIC_KEY, this.mirroredSourceTopicName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.STREAM_TOPIC_KEY, this.streamsTopicTargetName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.SCRAPER_KEY, this.scraperName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.PRODUCER_KEY, this.producerName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.CONSUMER_KEY, this.consumerName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.ADMIN_KEY, this.adminName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.USER_NAME_KEY, this.username);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.SOURCE_USER_NAME_KEY, this.sourceUsername);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.TARGET_USER_NAME_KEY, this.targetUsername);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.KAFKA_USER_NAME_KEY, this.kafkaUsername);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.ENTITY_OPERATOR_NAME_KEY, this.eoDeploymentName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.KAFKA_STATEFULSET_NAME_KEY, this.kafkaStatefulSetName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.ZOOKEEPER_STATEFULSET_NAME_KEY, this.zkStatefulSetName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.KAFKA_SELECTOR_KEY, this.kafkaSelector);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.ZOOKEEPER_SELECTOR_KEY, this.zkSelector);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.KAFKA_CONNECT_SELECTOR_KEY, this.kafkaConnectSelector);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.MM2_SELECTOR_KEY, this.mm2Selector);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.MESSAGE_COUNT_KEY, this.messageCount);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.TEST_EXECUTION_START_TIME_KEY, this.testExecutionStartTime);
    }

    public void addToTestStorage(String key, Object value) {
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(key, value);
    }

    public Object retrieveFromTestStorage(String key) {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(key);
    }

    public String getTestName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TEST_NAME_KEY).toString();
    }

    public String getNamespaceName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.NAMESPACE_KEY).toString();
    }

    public String getClusterName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString();
    }

    public String getKafkaNodePoolName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_NODE_POOL_KEY).toString();
    }

    public String getSourceClusterName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.SOURCE_CLUSTER_KEY).toString();
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

    public String getMirroredSourceTopicName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.MIRRORED_SOURCE_TOPIC_KEY).toString();
    }

    public String getStreamsTopicTargetName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.STREAM_TOPIC_KEY).toString();
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

    public String getUsername() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.USER_NAME_KEY).toString();
    }

    public String getSourceUsername() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.SOURCE_USER_NAME_KEY).toString();
    }

    public String getTargetUsername() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TARGET_USER_NAME_KEY).toString();
    }

    public String getKafkaUsername() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_USER_NAME_KEY).toString();
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
        return (LabelSelector) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_SELECTOR_KEY);
    }

    public LabelSelector getZookeeperSelector() {
        return (LabelSelector) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.ZOOKEEPER_SELECTOR_KEY);
    }

    public LabelSelector getKafkaConnectSelector() {
        return (LabelSelector) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CONNECT_SELECTOR_KEY);
    }

    public LabelSelector getMM2Selector() {
        return (LabelSelector) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.MM2_SELECTOR_KEY);
    }

    public int getMessageCount() {
        return (int) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.MESSAGE_COUNT_KEY);
    }

    public long getTestExecutionStartTime() {
        return (long) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TEST_EXECUTION_START_TIME_KEY);
    }

    public KafkaTracingClients getTracingClients() {
        return (KafkaTracingClients) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_TRACING_CLIENT_KEY);
    }

    public String getScraperPodName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.SCRAPER_POD_KEY).toString();
    }
}
