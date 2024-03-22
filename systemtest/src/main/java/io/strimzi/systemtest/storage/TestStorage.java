/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.storage;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaTracingClients;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.StrimziPodSetResource;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.time.Duration;
import java.util.Random;

import static io.strimzi.operator.common.Util.hashStub;
import static io.strimzi.systemtest.TestConstants.CONTINUOUS_MESSAGE_COUNT;
import static io.strimzi.systemtest.TestConstants.MESSAGE_COUNT;

/**
 * TestStorage generate and stores values in the specific @see{ExtensionContext}. This ensures that if one want to
 * retrieve data from TestStorage it can be done via ExtensionContext (with help of @ConcurrentHashMap)
 */
final public class TestStorage {

    private static final String PRODUCER = "producer";
    private static final String CONSUMER = "consumer";
    private static final String CONTINUOUS_SUFFIX = "-continuous";
    private static final String ADMIN = "admin-client";
    private static final String USER = "user";
    private static final String CLUSTER_NAME_PREFIX = "cluster-";
    private static final Random RANDOM = new Random();

    private ExtensionContext extensionContext;
    private String testName;
    private String namespaceName;
    private String clusterName;
    private String brokerPoolName;
    private String controllerPoolName;
    private String mixedPoolName;
    private String sourceClusterName;
    private String sourceBrokerPoolName;
    private String sourceControllerPoolName;
    private String targetClusterName;
    private String targetBrokerPoolName;
    private String targetControllerPoolName;
    private String topicName;
    private String targetTopicName;
    private String mirroredSourceTopicName;
    private String streamsTopicTargetName;
    private String scraperName;
    private String producerName;
    private String consumerName;
    private String continuousProducerName;
    private String continuousConsumerName;
    private String continuousTopicName;
    private String adminName;
    private String username;
    private String sourceUsername;
    private String targetUsername;
    private String kafkaUsername;
    private String eoDeploymentName;
    private String brokerComponentName;
    private String controllerComponentName;
    private String mixedComponentName;
    private LabelSelector brokerSelector;
    private LabelSelector brokerPoolSelector;
    private LabelSelector controllerPoolSelector;
    private LabelSelector mixedPoolSelector;
    private LabelSelector controllerSelector;
    private LabelSelector mixedSelector;
    private LabelSelector kafkaConnectSelector;
    private LabelSelector mm2Selector;
    private int messageCount;
    private int continuousMessageCount;
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
        this.brokerPoolName = TestConstants.BROKER_ROLE_PREFIX + hashStub(clusterName);
        this.controllerPoolName = TestConstants.CONTROLLER_ROLE_PREFIX + hashStub(clusterName);
        this.mixedPoolName = TestConstants.MIXED_ROLE_PREFIX + hashStub(clusterName);
        this.sourceClusterName = clusterName + "-source";
        this.sourceBrokerPoolName = TestConstants.BROKER_ROLE_PREFIX + hashStub(sourceClusterName);
        this.sourceControllerPoolName = TestConstants.CONTROLLER_ROLE_PREFIX + hashStub(sourceClusterName);
        this.targetClusterName = clusterName + "-target";
        this.targetBrokerPoolName = TestConstants.BROKER_ROLE_PREFIX + hashStub(targetClusterName);
        this.targetControllerPoolName = TestConstants.CONTROLLER_ROLE_PREFIX + hashStub(targetClusterName);
        this.topicName = KafkaTopicUtils.generateRandomNameOfTopic();
        this.continuousTopicName = topicName + CONTINUOUS_SUFFIX;
        this.targetTopicName = topicName + "-target";
        this.mirroredSourceTopicName = sourceClusterName + "." + topicName;
        this.streamsTopicTargetName = KafkaTopicUtils.generateRandomNameOfTopic();
        this.scraperName = clusterName + "-" + TestConstants.SCRAPER_NAME;
        this.producerName = clusterName + "-" + PRODUCER;
        this.consumerName = clusterName  + "-" + CONSUMER;
        this.continuousProducerName = producerName + CONTINUOUS_SUFFIX;
        this.continuousConsumerName = consumerName + CONTINUOUS_SUFFIX;
        this.adminName = clusterName + "-" + ADMIN;
        this.username = clusterName + "-" + USER;
        this.sourceUsername = username + "-source";
        this.targetUsername = username + "-target";
        this.kafkaUsername = KafkaUserUtils.generateRandomNameOfKafkaUser();
        this.eoDeploymentName = KafkaResources.entityOperatorDeploymentName(clusterName);
        this.brokerComponentName = StrimziPodSetResource.getBrokerComponentName(clusterName);
        this.controllerComponentName = StrimziPodSetResource.getControllerComponentName(clusterName);
        this.mixedComponentName = KafkaResource.getStrimziPodSetName(clusterName, KafkaNodePoolResource.getMixedPoolName(clusterName));
        this.brokerSelector = KafkaResource.getLabelSelector(clusterName, brokerComponentName);
        this.brokerPoolSelector = KafkaNodePoolResource.getLabelSelector(clusterName, this.brokerPoolName, ProcessRoles.BROKER);
        this.controllerPoolSelector = KafkaNodePoolResource.getLabelSelector(clusterName, this.controllerPoolName, ProcessRoles.CONTROLLER);
        this.mixedPoolSelector = KafkaNodePoolResource.getLabelSelector(clusterName, this.mixedPoolName, ProcessRoles.CONTROLLER);
        this.controllerSelector = KafkaResource.getLabelSelector(clusterName, controllerComponentName);
        this.mixedSelector = KafkaResource.getLabelSelector(clusterName, mixedComponentName);
        this.kafkaConnectSelector = KafkaConnectResource.getLabelSelector(clusterName, KafkaConnectResources.componentName(clusterName));
        this.mm2Selector = KafkaMirrorMaker2Resource.getLabelSelector(clusterName, KafkaMirrorMaker2Resources.componentName(clusterName));
        this.messageCount = messageCount;
        this.continuousMessageCount = CONTINUOUS_MESSAGE_COUNT;
        this.testExecutionStartTime = System.currentTimeMillis();

        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.TEST_NAME_KEY, this.testName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.NAMESPACE_KEY, this.namespaceName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.CLUSTER_KEY, this.clusterName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.BROKER_POOL_KEY, this.brokerPoolName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.CONTROLLER_POOL_KEY, this.controllerPoolName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.MIXED_POOL_KEY, this.mixedPoolName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.SOURCE_CLUSTER_KEY, this.sourceClusterName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.SOURCE_BROKER_POOL_KEY, this.sourceBrokerPoolName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.SOURCE_CONTROLLER_POOL_KEY, this.sourceControllerPoolName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.TARGET_CLUSTER_KEY, this.targetClusterName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.TARGET_BROKER_POOL_KEY, this.targetBrokerPoolName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.TARGET_CONTROLLER_POOL_KEY, this.targetControllerPoolName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.TOPIC_KEY, this.topicName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.CONTINUOUS_TOPIC_KEY, this.continuousTopicName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.TARGET_TOPIC_KEY, this.targetTopicName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.MIRRORED_SOURCE_TOPIC_KEY, this.mirroredSourceTopicName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.STREAM_TOPIC_KEY, this.streamsTopicTargetName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.SCRAPER_KEY, this.scraperName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.PRODUCER_KEY, this.producerName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.CONTINUOUS_PRODUCER_KEY, this.continuousProducerName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.CONSUMER_KEY, this.consumerName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.CONTINUOUS_CONSUMER_KEY, this.continuousConsumerName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.ADMIN_KEY, this.adminName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.USER_NAME_KEY, this.username);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.SOURCE_USER_NAME_KEY, this.sourceUsername);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.TARGET_USER_NAME_KEY, this.targetUsername);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.KAFKA_USER_NAME_KEY, this.kafkaUsername);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.ENTITY_OPERATOR_NAME_KEY, this.eoDeploymentName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.BROKER_COMPONENT_NAME_KEY, this.brokerComponentName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.CONTROLLER_COMPONENT_NAME_KEY, this.controllerComponentName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.MIXED_COMPONENT_NAME_KEY, this.mixedComponentName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.BROKER_SELECTOR_KEY, this.brokerSelector);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.BROKER_POOL_SELECTOR_KEY, this.brokerPoolSelector);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.CONTROLLER_POOL_SELECTOR_KEY, this.controllerPoolSelector);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.MIXED_POOL_SELECTOR_KEY, this.mixedPoolSelector);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.CONTROLLER_SELECTOR_KEY, this.controllerSelector);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.MIXED_SELECTOR_KEY, this.mixedSelector);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.KAFKA_CONNECT_SELECTOR_KEY, this.kafkaConnectSelector);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.MM2_SELECTOR_KEY, this.mm2Selector);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.MESSAGE_COUNT_KEY, this.messageCount);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.CONTINUOUS_MESSAGE_COUNT_KEY, this.continuousMessageCount);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.TEST_EXECUTION_START_TIME_KEY, this.testExecutionStartTime);
    }

    public void addToTestStorage(String key, Object value) {
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(key, value);
    }

    public Object retrieveFromTestStorage(String key) {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(key);
    }

    public ExtensionContext getExtensionContext() {
        return extensionContext;
    }

    public String getTestName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.TEST_NAME_KEY).toString();
    }

    public String getNamespaceName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.NAMESPACE_KEY).toString();
    }

    public String getClusterName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.CLUSTER_KEY).toString();
    }

    public String getBrokerPoolName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.BROKER_POOL_KEY).toString();
    }

    public String getControllerPoolName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.CONTROLLER_POOL_KEY).toString();
    }


    public String getMixedPoolName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.MIXED_POOL_KEY).toString();
    }

    public String getSourceClusterName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.SOURCE_CLUSTER_KEY).toString();
    }

    public String getSourceBrokerPoolName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.SOURCE_BROKER_POOL_KEY).toString();
    }

    public String getSourceControllerPoolName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.SOURCE_CONTROLLER_POOL_KEY).toString();
    }

    public String getTargetClusterName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.TARGET_CLUSTER_KEY).toString();
    }

    public String getTargetBrokerPoolName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.TARGET_BROKER_POOL_KEY).toString();
    }

    public String getTargetControllerPoolName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.TARGET_CONTROLLER_POOL_KEY).toString();
    }

    public String getTopicName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.TOPIC_KEY).toString();
    }

    public String getContinuousTopicName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.CONTINUOUS_TOPIC_KEY).toString();
    }

    public String getTargetTopicName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.TARGET_TOPIC_KEY).toString();
    }

    public String getMirroredSourceTopicName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.MIRRORED_SOURCE_TOPIC_KEY).toString();
    }

    public String getStreamsTopicTargetName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.STREAM_TOPIC_KEY).toString();
    }

    public String getScraperName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.SCRAPER_KEY).toString();
    }

    public String getProducerName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.PRODUCER_KEY).toString();
    }

    public String getContinuousProducerName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.CONTINUOUS_PRODUCER_KEY).toString();
    }

    public String getConsumerName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.CONSUMER_KEY).toString();
    }

    public String getContinuousConsumerName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.CONTINUOUS_CONSUMER_KEY).toString();
    }

    public String getAdminName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.ADMIN_KEY).toString();
    }

    public String getUsername() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.USER_NAME_KEY).toString();
    }

    public String getSourceUsername() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.SOURCE_USER_NAME_KEY).toString();
    }

    public String getTargetUsername() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.TARGET_USER_NAME_KEY).toString();
    }

    public String getKafkaUsername() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.KAFKA_USER_NAME_KEY).toString();
    }

    public String getEoDeploymentName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.ENTITY_OPERATOR_NAME_KEY).toString();
    }

    public String getBrokerComponentName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.BROKER_COMPONENT_NAME_KEY).toString();
    }
    public String getControllerComponentName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.CONTROLLER_COMPONENT_NAME_KEY).toString();
    }

    public String getMixedComponentName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.MIXED_COMPONENT_NAME_KEY).toString();
    }

    public LabelSelector getBrokerSelector() {
        return (LabelSelector) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.BROKER_SELECTOR_KEY);
    }

    public LabelSelector getBrokerPoolSelector() {
        return (LabelSelector) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.BROKER_POOL_SELECTOR_KEY);
    }

    public LabelSelector getControllerPoolSelector() {
        return (LabelSelector) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.CONTROLLER_POOL_SELECTOR_KEY);
    }

    public LabelSelector getMixedPoolSelector() {
        return (LabelSelector) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.MIXED_POOL_SELECTOR_KEY);
    }

    public LabelSelector getControllerSelector() {
        return (LabelSelector) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.CONTROLLER_SELECTOR_KEY);
    }

    public LabelSelector getMixedSelector() {
        return (LabelSelector) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.MIXED_SELECTOR_KEY);
    }

    public LabelSelector getKafkaConnectSelector() {
        return (LabelSelector) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.KAFKA_CONNECT_SELECTOR_KEY);
    }

    public LabelSelector getMM2Selector() {
        return (LabelSelector) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.MM2_SELECTOR_KEY);
    }

    public int getMessageCount() {
        return (int) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.MESSAGE_COUNT_KEY);
    }

    public int getContinuousMessageCount() {
        return (int) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.CONTINUOUS_MESSAGE_COUNT_KEY);
    }

    public long getTestExecutionStartTime() {
        return (long) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.TEST_EXECUTION_START_TIME_KEY);
    }

    public long getTestExecutionTimeInSeconds() {
        return Duration.ofMillis(System.currentTimeMillis() - getTestExecutionStartTime()).getSeconds();
    }

    public KafkaTracingClients getTracingClients() {
        return (KafkaTracingClients) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.KAFKA_TRACING_CLIENT_KEY);
    }

    public String getScraperPodName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.SCRAPER_POD_KEY).toString();
    }
}
