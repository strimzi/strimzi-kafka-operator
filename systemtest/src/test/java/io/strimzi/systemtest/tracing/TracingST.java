/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.tracing;

import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyBuilder;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBridgeExampleClients;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaTracingExampleClients;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.FileUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.specific.TracingUtils;
import io.strimzi.test.TestUtils;
import io.vertx.junit5.VertxExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectS2IResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMakerResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.CONNECT_S2I;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER2;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.TRACING;
import static io.strimzi.systemtest.bridge.HttpBridgeAbstractST.bridgePort;
import static io.strimzi.systemtest.bridge.HttpBridgeAbstractST.bridgeServiceName;
import static io.strimzi.test.TestUtils.getFileAsString;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@Tag(REGRESSION)
@Tag(TRACING)
@Tag(INTERNAL_CLIENTS_USED)
@ExtendWith(VertxExtension.class)
public class TracingST extends AbstractST {

    private static final String NAMESPACE = "tracing-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(TracingST.class);

    private static final String JAEGER_INSTANCE_PATH = TestUtils.USER_PATH + "/../systemtest/src/test/resources/tracing/jaeger-instance.yaml";

    private static final String JAEGER_PRODUCER_SERVICE = "hello-world-producer";
    private static final String JAEGER_CONSUMER_SERVICE = "hello-world-consumer";
    private static final String JAEGER_KAFKA_STREAMS_SERVICE = "hello-world-streams";
    private static final String JAEGER_MIRROR_MAKER_SERVICE = "my-mirror-maker";
    private static final String JAEGER_MIRROR_MAKER2_SERVICE = "my-mirror-maker2";
    private static final String JAEGER_KAFKA_CONNECT_SERVICE = "my-connect";
    private static final String JAEGER_KAFKA_CONNECT_S2I_SERVICE = "my-connect-s2i";
    private static final String JAEGER_KAFKA_BRIDGE_SERVICE = "my-kafka-bridge";

    protected static final String PRODUCER_JOB_NAME = "hello-world-producer";
    protected static final String CONSUMER_JOB_NAME = "hello-world-consumer";

    private static final String JAEGER_AGENT_NAME = "my-jaeger-agent";
    private static final String JAEGER_SAMPLER_TYPE = "const";
    private static final String JAEGER_SAMPLER_PARAM = "1";
    private static final String JAEGER_OPERATOR_DEPLOYMENT_NAME = "jaeger-operator";
    private static final String JAEGER_INSTANCE_NAME = "my-jaeger";
    private static final String JAEGER_VERSION = "1.21.3";

    private static final String TOPIC_NAME = "my-topic";
    private static final String TOPIC_TARGET_NAME = "cipot-ym";

    private Stack<String> jaegerConfigs = new Stack<>();

    private String kafkaClientsPodName;

    private static KafkaTracingExampleClients kafkaTracingClient;

    @Test
    void testProducerService() {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        Map<String, Object> configOfSourceKafka = new HashMap<>();
        configOfSourceKafka.put("offsets.topic.replication.factor", "1");
        configOfSourceKafka.put("transaction.state.log.replication.factor", "1");
        configOfSourceKafka.put("transaction.state.log.min.isr", "1");

        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(clusterName, 3, 1)
                .editSpec()
                    .editKafka()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endKafka()
                    .editZookeeper()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .build());

        KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(clusterName, TOPIC_NAME)
                .editSpec()
                    .withReplicas(1)
                    .withPartitions(12)
                .endSpec()
                .build());

        kafkaTracingClient.createAndWaitForReadiness(kafkaTracingClient.producerWithTracing().build());

        TracingUtils.verify(JAEGER_PRODUCER_SERVICE, kafkaClientsPodName);

        LOGGER.info("Deleting topic {} from CR", TOPIC_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(TOPIC_NAME);
    }

    @Test
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testConnectService() {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(clusterName, 3, 1)
                .editSpec()
                    .editKafka()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endKafka()
                    .editZookeeper()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .build());

        Map<String, Object> configOfKafkaConnect = new HashMap<>();
        configOfKafkaConnect.put("config.storage.replication.factor", "1");
        configOfKafkaConnect.put("offset.storage.replication.factor", "1");
        configOfKafkaConnect.put("status.storage.replication.factor", "1");
        configOfKafkaConnect.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("key.converter.schemas.enable", "false");
        configOfKafkaConnect.put("value.converter.schemas.enable", "false");

        KafkaConnectResource.createAndWaitForReadiness(KafkaConnectResource.kafkaConnect(clusterName, 1)
                .withNewSpec()
                    .withConfig(configOfKafkaConnect)
                    .withNewJaegerTracing()
                    .endJaegerTracing()
                    .withBootstrapServers(KafkaResources.plainBootstrapAddress(clusterName))
                    .withReplicas(1)
                    .withNewTemplate()
                        .withNewConnectContainer()
                            .addNewEnv()
                                .withName("JAEGER_SERVICE_NAME")
                                .withValue(JAEGER_KAFKA_CONNECT_SERVICE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_AGENT_HOST")
                                .withValue(JAEGER_AGENT_NAME)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_TYPE")
                                .withValue(JAEGER_SAMPLER_TYPE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_PARAM")
                                .withValue(JAEGER_SAMPLER_PARAM)
                            .endEnv()
                        .endConnectContainer()
                    .endTemplate()
                .endSpec()
                .build());

        String kafkaConnectPodName = kubeClient().listPods(Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();
        String pathToConnectorSinkConfig = TestUtils.USER_PATH + "/../systemtest/src/test/resources/file/sink/connector.json";
        String connectorConfig = getFileAsString(pathToConnectorSinkConfig);

        LOGGER.info("Creating file sink in {}", pathToConnectorSinkConfig);
        cmdKubeClient().execInPod(kafkaConnectPodName, "/bin/bash", "-c", "curl -X POST -H \"Content-Type: application/json\" --data "
                + "'" + connectorConfig + "'" + " http://localhost:8083/connectors");

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(TEST_TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

        TracingUtils.verify(JAEGER_KAFKA_CONNECT_SERVICE, kafkaClientsPodName);

        LOGGER.info("Deleting topic {} from CR", TEST_TOPIC_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TEST_TOPIC_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(TEST_TOPIC_NAME);
    }

    @Test
    void testProducerWithStreamsService() {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        Map<String, Object> configOfSourceKafka = new HashMap<>();
        configOfSourceKafka.put("offsets.topic.replication.factor", "1");
        configOfSourceKafka.put("transaction.state.log.replication.factor", "1");
        configOfSourceKafka.put("transaction.state.log.min.isr", "1");

        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(clusterName, 3, 1)
                .editSpec()
                    .editKafka()
                        .withConfig(configOfSourceKafka)
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endKafka()
                    .editZookeeper()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .build());

        KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(clusterName, TOPIC_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .build());

        KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(clusterName, TOPIC_TARGET_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .build());

        kafkaTracingClient.createAndWaitForReadiness(kafkaTracingClient.producerWithTracing().build());

        TracingUtils.verify(JAEGER_PRODUCER_SERVICE, kafkaClientsPodName);

        kafkaTracingClient.createAndWaitForReadiness(kafkaTracingClient.kafkaStreamsWithTracing().build());

        TracingUtils.verify(JAEGER_KAFKA_STREAMS_SERVICE, kafkaClientsPodName);

        LOGGER.info("Deleting topic {} from CR", TOPIC_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(TOPIC_NAME);

        LOGGER.info("Deleting topic {} from CR", TOPIC_TARGET_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_TARGET_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(TOPIC_TARGET_NAME);
    }

    @Test
    void testProducerConsumerService() {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        Map<String, Object> configOfSourceKafka = new HashMap<>();
        configOfSourceKafka.put("offsets.topic.replication.factor", "1");
        configOfSourceKafka.put("transaction.state.log.replication.factor", "1");
        configOfSourceKafka.put("transaction.state.log.min.isr", "1");

        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(clusterName, 3, 1)
                .editSpec()
                    .editKafka()
                        .withConfig(configOfSourceKafka)
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endKafka()
                    .editZookeeper()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .build());

        KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(clusterName, TOPIC_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .build());

        kafkaTracingClient.createAndWaitForReadiness(kafkaTracingClient.producerWithTracing().build());

        TracingUtils.verify(JAEGER_PRODUCER_SERVICE, kafkaClientsPodName);

        kafkaTracingClient.createAndWaitForReadiness(kafkaTracingClient.consumerWithTracing().build());

        TracingUtils.verify(JAEGER_CONSUMER_SERVICE, kafkaClientsPodName);

        LOGGER.info("Deleting topic {} from CR", TOPIC_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(TOPIC_NAME);
    }

    @Test
    @Tag(ACCEPTANCE)
    void testProducerConsumerStreamsService() {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        Map<String, Object> configOfSourceKafka = new HashMap<>();
        configOfSourceKafka.put("offsets.topic.replication.factor", "1");
        configOfSourceKafka.put("transaction.state.log.replication.factor", "1");
        configOfSourceKafka.put("transaction.state.log.min.isr", "1");

        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(clusterName, 3, 1)
                .editSpec()
                    .editKafka()
                        .withConfig(configOfSourceKafka)
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endKafka()
                    .editZookeeper()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .build());

        KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(clusterName, TOPIC_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .build());


        KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(clusterName, TOPIC_TARGET_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .build());

        kafkaTracingClient.createAndWaitForReadiness(kafkaTracingClient.producerWithTracing().build());

        TracingUtils.verify(JAEGER_PRODUCER_SERVICE, kafkaClientsPodName);

        kafkaTracingClient.createAndWaitForReadiness(kafkaTracingClient.consumerWithTracing().build());

        TracingUtils.verify(JAEGER_CONSUMER_SERVICE, kafkaClientsPodName);

        kafkaTracingClient.createAndWaitForReadiness(kafkaTracingClient.kafkaStreamsWithTracing().build());

        TracingUtils.verify(JAEGER_KAFKA_STREAMS_SERVICE, kafkaClientsPodName);

        LOGGER.info("Deleting topic {} from CR", TOPIC_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(TOPIC_NAME);

        LOGGER.info("Deleting topic {} from CR", TOPIC_TARGET_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_TARGET_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(TOPIC_TARGET_NAME);
    }

    @Test
    @Tag(MIRROR_MAKER2)
    void testProducerConsumerMirrorMaker2Service() {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        final String kafkaClusterSourceName = clusterName + "-source";
        final String kafkaClusterTargetName = clusterName + "-target";

        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(kafkaClusterSourceName, 3, 1)
                .editSpec()
                    .editKafka()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endKafka()
                    .editZookeeper()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .build());

        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(kafkaClusterTargetName, 3, 1)
                .editSpec()
                    .editKafka()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endKafka()
                    .editZookeeper()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .build());

        // Create topic and deploy clients before Mirror Maker to not wait for MM to find the new topics
        KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(kafkaClusterSourceName, TOPIC_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .build());

        KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(kafkaClusterTargetName, kafkaClusterSourceName + "." + TOPIC_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .build());

        LOGGER.info("Setting for kafka source plain bootstrap:{}", KafkaResources.plainBootstrapAddress(kafkaClusterSourceName));

        KafkaTracingExampleClients sourceKafkaTracingClient = kafkaTracingClient.toBuilder()
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName))
            .build();

        sourceKafkaTracingClient.createAndWaitForReadiness(sourceKafkaTracingClient.producerWithTracing().build());

        LOGGER.info("Setting for kafka target plain bootstrap:{}", KafkaResources.plainBootstrapAddress(kafkaClusterTargetName));

        KafkaTracingExampleClients targetKafkaTracingClient = kafkaTracingClient.toBuilder()
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))
            .withTopicName(kafkaClusterSourceName + "." + TOPIC_NAME)
            .build();

        targetKafkaTracingClient.createAndWaitForReadiness(targetKafkaTracingClient.consumerWithTracing().build());

        KafkaMirrorMaker2Resource.createAndWaitForReadiness(KafkaMirrorMaker2Resource.kafkaMirrorMaker2(clusterName, kafkaClusterTargetName, kafkaClusterSourceName, 1, false)
                .editMetadata()
                    .withName("my-mirror-maker2")
                .endMetadata()
                .editSpec()
                    .withNewJaegerTracing()
                    .endJaegerTracing()
                    .withNewTemplate()
                        .withNewConnectContainer()
                            .addNewEnv()
                                .withNewName("JAEGER_SERVICE_NAME")
                                .withValue(JAEGER_MIRROR_MAKER2_SERVICE)
                            .endEnv()
                            .addNewEnv()
                                .withNewName("JAEGER_AGENT_HOST")
                                .withValue(JAEGER_AGENT_NAME)
                            .endEnv()
                            .addNewEnv()
                                .withNewName("JAEGER_SAMPLER_TYPE")
                                .withValue(JAEGER_SAMPLER_TYPE)
                            .endEnv()
                            .addNewEnv()
                                .withNewName("JAEGER_SAMPLER_PARAM")
                                .withValue(JAEGER_SAMPLER_PARAM)
                            .endEnv()
                        .endConnectContainer()
                    .endTemplate()
                .endSpec()
                .build());

        TracingUtils.verify(JAEGER_PRODUCER_SERVICE, kafkaClientsPodName, "To_" + TOPIC_NAME);
        TracingUtils.verify(JAEGER_CONSUMER_SERVICE, kafkaClientsPodName, "From_" + kafkaClusterSourceName + "." + TOPIC_NAME);
        TracingUtils.verify(JAEGER_MIRROR_MAKER2_SERVICE, kafkaClientsPodName, "From_" + TOPIC_NAME);
        TracingUtils.verify(JAEGER_MIRROR_MAKER2_SERVICE, kafkaClientsPodName, "To_" + kafkaClusterSourceName + "." + TOPIC_NAME);
    }

    @Test
    @Tag(MIRROR_MAKER)
    void testProducerConsumerMirrorMakerService() {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        final String kafkaClusterSourceName = clusterName + "-source";
        final String kafkaClusterTargetName = clusterName + "-target";

        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(kafkaClusterSourceName, 3, 1)
                .editSpec()
                    .editKafka()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endKafka()
                    .editZookeeper()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .build());

        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(kafkaClusterTargetName, 3, 1)
                .editSpec()
                    .editKafka()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endKafka()
                    .editZookeeper()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .build());

        // Create topic and deploy clients before Mirror Maker to not wait for MM to find the new topics
        KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(kafkaClusterSourceName, TOPIC_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .build());

        KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(kafkaClusterTargetName, TOPIC_NAME + "-target")
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                    .withTopicName(TOPIC_NAME)
                .endSpec()
                .build());

        LOGGER.info("Setting for kafka source plain bootstrap:{}", KafkaResources.plainBootstrapAddress(kafkaClusterSourceName));

        KafkaTracingExampleClients sourceKafkaTracingClient = kafkaTracingClient.toBuilder()
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName))
            .build();

        sourceKafkaTracingClient.createAndWaitForReadiness(sourceKafkaTracingClient.producerWithTracing().build());

        LOGGER.info("Setting for kafka target plain bootstrap:{}", KafkaResources.plainBootstrapAddress(kafkaClusterTargetName));

        KafkaTracingExampleClients targetKafkaTracingClient =  kafkaTracingClient.toBuilder()
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))
            .build();

        targetKafkaTracingClient.createAndWaitForReadiness(targetKafkaTracingClient.consumerWithTracing().build());

        KafkaMirrorMakerResource.createAndWaitForReadiness(KafkaMirrorMakerResource.kafkaMirrorMaker(clusterName, kafkaClusterSourceName, kafkaClusterTargetName,
            ClientUtils.generateRandomConsumerGroup(), 1, false)
                .editMetadata()
                    .withName("my-mirror-maker")
                .endMetadata()
                .editSpec()
                    .withNewJaegerTracing()
                    .endJaegerTracing()
                    .withNewTemplate()
                        .withNewMirrorMakerContainer()
                            .addNewEnv()
                                .withNewName("JAEGER_SERVICE_NAME")
                                .withValue(JAEGER_MIRROR_MAKER_SERVICE)
                            .endEnv()
                            .addNewEnv()
                                .withNewName("JAEGER_AGENT_HOST")
                                .withValue(JAEGER_AGENT_NAME)
                            .endEnv()
                            .addNewEnv()
                                .withNewName("JAEGER_SAMPLER_TYPE")
                                .withValue(JAEGER_SAMPLER_TYPE)
                            .endEnv()
                            .addNewEnv()
                                .withNewName("JAEGER_SAMPLER_PARAM")
                                .withValue(JAEGER_SAMPLER_PARAM)
                            .endEnv()
                        .endMirrorMakerContainer()
                    .endTemplate()
                .endSpec()
                .build());

        TracingUtils.verify(JAEGER_PRODUCER_SERVICE, kafkaClientsPodName, "To_" + TOPIC_NAME);
        TracingUtils.verify(JAEGER_CONSUMER_SERVICE, kafkaClientsPodName, "From_" + TOPIC_NAME);
        TracingUtils.verify(JAEGER_MIRROR_MAKER_SERVICE, kafkaClientsPodName, "From_" + TOPIC_NAME);
        TracingUtils.verify(JAEGER_MIRROR_MAKER_SERVICE, kafkaClientsPodName, "To_" + TOPIC_NAME);
    }

    @Test
    @Tag(CONNECT)
    @Tag(MIRROR_MAKER)
    @Tag(CONNECT_COMPONENTS)
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testProducerConsumerMirrorMakerConnectStreamsService() {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        final String kafkaClusterSourceName = clusterName + "-source";
        final String kafkaClusterTargetName = clusterName + "-target";

        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(kafkaClusterSourceName, 3, 1).build());
        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(kafkaClusterTargetName, 3, 1).build());

        // Create topic and deploy clients before Mirror Maker to not wait for MM to find the new topics
        KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(kafkaClusterSourceName, TOPIC_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .build());

        KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(kafkaClusterSourceName, TOPIC_TARGET_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .build());

        KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(kafkaClusterTargetName, TOPIC_NAME + "-target")
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                    .withTopicName(TOPIC_NAME)
                .endSpec()
                .build());

        KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(kafkaClusterTargetName, TOPIC_TARGET_NAME + "-target")
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                    .withTopicName(TOPIC_TARGET_NAME)
                .endSpec()
                .build());

        KafkaTracingExampleClients sourceKafkaTracingClient = kafkaTracingClient.toBuilder()
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName))
            .build();

        sourceKafkaTracingClient.createAndWaitForReadiness(sourceKafkaTracingClient.producerWithTracing().build());

        KafkaTracingExampleClients targetKafkaTracingClient = kafkaTracingClient.toBuilder()
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))
            .build();

        targetKafkaTracingClient.createAndWaitForReadiness(targetKafkaTracingClient.consumerWithTracing().build());

        sourceKafkaTracingClient.createAndWaitForReadiness(sourceKafkaTracingClient.kafkaStreamsWithTracing().build());

        Map<String, Object> configOfKafkaConnect = new HashMap<>();
        configOfKafkaConnect.put("config.storage.replication.factor", "1");
        configOfKafkaConnect.put("offset.storage.replication.factor", "1");
        configOfKafkaConnect.put("status.storage.replication.factor", "1");
        configOfKafkaConnect.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("key.converter.schemas.enable", "false");
        configOfKafkaConnect.put("value.converter.schemas.enable", "false");

        KafkaConnectResource.createAndWaitForReadiness(KafkaConnectResource.kafkaConnect(clusterName, 1)
                .withNewSpec()
                    .withConfig(configOfKafkaConnect)
                    .withNewJaegerTracing()
                    .endJaegerTracing()
                    .withBootstrapServers(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))
                    .withReplicas(1)
                    .withNewTemplate()
                        .withNewConnectContainer()
                            .addNewEnv()
                                .withName("JAEGER_SERVICE_NAME")
                                .withValue(JAEGER_KAFKA_CONNECT_SERVICE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_AGENT_HOST")
                                .withValue(JAEGER_AGENT_NAME)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_TYPE")
                                .withValue(JAEGER_SAMPLER_TYPE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_PARAM")
                                .withValue(JAEGER_SAMPLER_PARAM)
                            .endEnv()
                        .endConnectContainer()
                    .endTemplate()
                .endSpec()
                .build());


        String kafkaConnectPodName = kubeClient().listPods(Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();
        String pathToConnectorSinkConfig = TestUtils.USER_PATH + "/../systemtest/src/test/resources/file/sink/connector.json";
        String connectorConfig = getFileAsString(pathToConnectorSinkConfig);

        LOGGER.info("Creating file sink in {}", pathToConnectorSinkConfig);
        cmdKubeClient().execInPod(kafkaConnectPodName, "/bin/bash", "-c", "curl -X POST -H \"Content-Type: application/json\" --data "
                + "'" + connectorConfig + "'" + " http://localhost:8083/connectors");

        KafkaMirrorMakerResource.createAndWaitForReadiness(KafkaMirrorMakerResource.kafkaMirrorMaker(clusterName, kafkaClusterSourceName, kafkaClusterTargetName,
            ClientUtils.generateRandomConsumerGroup(), 1, false)
                .editMetadata()
                    .withName("my-mirror-maker")
                .endMetadata()
                .editSpec()
                    .withNewJaegerTracing()
                    .endJaegerTracing()
                    .withNewTemplate()
                        .withNewMirrorMakerContainer()
                            .addNewEnv()
                                .withNewName("JAEGER_SERVICE_NAME")
                                .withValue("my-mirror-maker")
                            .endEnv()
                            .addNewEnv()
                                .withNewName("JAEGER_AGENT_HOST")
                                .withValue(JAEGER_AGENT_NAME)
                            .endEnv()
                            .addNewEnv()
                                .withNewName("JAEGER_SAMPLER_TYPE")
                                .withValue(JAEGER_SAMPLER_TYPE)
                            .endEnv()
                            .addNewEnv()
                                .withNewName("JAEGER_SAMPLER_PARAM")
                                .withValue(JAEGER_SAMPLER_PARAM)
                            .endEnv()
                        .endMirrorMakerContainer()
                    .endTemplate()
                .endSpec()
                .build());

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(TEST_TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(kafkaClusterTargetName)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

        TracingUtils.verify(JAEGER_PRODUCER_SERVICE, kafkaClientsPodName, "To_" + TOPIC_NAME);
        TracingUtils.verify(JAEGER_CONSUMER_SERVICE, kafkaClientsPodName, "From_" + TOPIC_NAME);
        TracingUtils.verify(JAEGER_KAFKA_CONNECT_SERVICE, kafkaClientsPodName, "From_" + TOPIC_NAME);
        TracingUtils.verify(JAEGER_KAFKA_STREAMS_SERVICE, kafkaClientsPodName, "From_" + TOPIC_NAME);
        TracingUtils.verify(JAEGER_KAFKA_STREAMS_SERVICE, kafkaClientsPodName, "To_" + TOPIC_TARGET_NAME);
        TracingUtils.verify(JAEGER_MIRROR_MAKER_SERVICE, kafkaClientsPodName, "From_" + TOPIC_NAME);
        TracingUtils.verify(JAEGER_MIRROR_MAKER_SERVICE, kafkaClientsPodName, "To_" + TOPIC_NAME);
        TracingUtils.verify(JAEGER_MIRROR_MAKER_SERVICE, kafkaClientsPodName, "From_" + TOPIC_TARGET_NAME);
        TracingUtils.verify(JAEGER_MIRROR_MAKER_SERVICE, kafkaClientsPodName, "To_" + TOPIC_TARGET_NAME);
    }

    @Test
    @OpenShiftOnly
    @Tag(CONNECT_S2I)
    @Tag(CONNECT_COMPONENTS)
    void testConnectS2IService() {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(clusterName, 3, 1).build());

        kafkaTracingClient.createAndWaitForReadiness(kafkaTracingClient.producerWithTracing().build());
        kafkaTracingClient.createAndWaitForReadiness(kafkaTracingClient.consumerWithTracing().build());

        final String kafkaConnectS2IName = "kafka-connect-s2i-name-1";

        KafkaClientsResource.createAndWaitForReadiness(KafkaClientsResource.deployKafkaClients(false, kafkaClientsName).build());

        Map<String, Object> configOfKafkaConnectS2I = new HashMap<>();
        configOfKafkaConnectS2I.put("key.converter.schemas.enable", "false");
        configOfKafkaConnectS2I.put("value.converter.schemas.enable", "false");
        configOfKafkaConnectS2I.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnectS2I.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");

        KafkaConnectS2IResource.createAndWaitForReadiness(KafkaConnectS2IResource.kafkaConnectS2I(kafkaConnectS2IName, clusterName, 1)
                .editSpec()
                    .withConfig(configOfKafkaConnectS2I)
                    .withNewJaegerTracing()
                    .endJaegerTracing()
                    .withNewTemplate()
                        .withNewConnectContainer()
                            .addNewEnv()
                                .withName("JAEGER_SERVICE_NAME")
                                .withValue(JAEGER_KAFKA_CONNECT_S2I_SERVICE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_AGENT_HOST")
                                .withValue(JAEGER_AGENT_NAME)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_TYPE")
                                .withValue(JAEGER_SAMPLER_TYPE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_PARAM")
                                .withValue(JAEGER_SAMPLER_PARAM)
                            .endEnv()
                        .endConnectContainer()
                    .endTemplate()
                .endSpec()
                .build());

        String kafkaConnectS2IPodName = kubeClient().listPods(Labels.STRIMZI_KIND_LABEL, KafkaConnectS2I.RESOURCE_KIND).get(0).getMetadata().getName();
        String execPodName = kubeClient().listPodsByPrefixInName(kafkaClientsName).get(0).getMetadata().getName();

        LOGGER.info("Creating FileSink connect via Pod:{}", execPodName);
        KafkaConnectorUtils.createFileSinkConnector(execPodName, TEST_TOPIC_NAME, Constants.DEFAULT_SINK_FILE_PATH,
                KafkaConnectResources.url(kafkaConnectS2IName, NAMESPACE, 8083));

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(TEST_TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(kafkaConnectS2IPodName, Constants.DEFAULT_SINK_FILE_PATH, "99");

        TracingUtils.verify(JAEGER_PRODUCER_SERVICE, kafkaClientsPodName);
        TracingUtils.verify(JAEGER_CONSUMER_SERVICE, kafkaClientsPodName);
        TracingUtils.verify(JAEGER_KAFKA_CONNECT_S2I_SERVICE, kafkaClientsPodName);

        LOGGER.info("Deleting topic {} from CR", TEST_TOPIC_NAME);
        cmdKubeClient().deleteByName(KafkaTopic.RESOURCE_KIND, TEST_TOPIC_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(TEST_TOPIC_NAME);
    }

    @Tag(NODEPORT_SUPPORTED)
    @Tag(BRIDGE)
    @Test
    void testKafkaBridgeService() {
        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(clusterName, 3, 1).build());

        // Deploy http bridge
        KafkaBridgeResource.createAndWaitForReadiness(KafkaBridgeResource.kafkaBridge(clusterName, KafkaResources.plainBootstrapAddress(clusterName), 1)
            .editSpec()
                .withNewJaegerTracing()
                .endJaegerTracing()
                    .withNewTemplate()
                        .withNewBridgeContainer()
                            .addNewEnv()
                                .withName("JAEGER_SERVICE_NAME")
                                .withValue(JAEGER_KAFKA_BRIDGE_SERVICE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_AGENT_HOST")
                                .withValue(JAEGER_AGENT_NAME)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_TYPE")
                                .withValue(JAEGER_SAMPLER_TYPE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_PARAM")
                                .withValue(JAEGER_SAMPLER_PARAM)
                            .endEnv()
                        .endBridgeContainer()
                    .endTemplate()
            .endSpec()
            .build());

        String bridgeProducer = "bridge-producer";
        KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(clusterName, TOPIC_NAME).build());

        KafkaBridgeExampleClients kafkaBridgeClientJob = new KafkaBridgeExampleClients.Builder()
            .withProducerName(bridgeProducer)
            .withBootstrapAddress(bridgeServiceName)
            .withTopicName(TOPIC_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withPort(bridgePort)
            .withDelayMs(1000)
            .withPollInterval(1000)
            .build();

        kafkaBridgeClientJob.createAndWaitForReadiness(kafkaBridgeClientJob.producerStrimziBridge().build());
        ClientUtils.waitForClientSuccess(bridgeProducer, NAMESPACE, MESSAGE_COUNT);

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        assertThat(internalKafkaClient.receiveMessagesPlain(), is(MESSAGE_COUNT));

        TracingUtils.verify(JAEGER_KAFKA_BRIDGE_SERVICE, kafkaClientsPodName);
    }

    /**
     * Delete Jaeger instance
     */
    void deleteJaeger() {
        while (!jaegerConfigs.empty()) {
            cmdKubeClient().namespace(cluster.getNamespace()).deleteContent(jaegerConfigs.pop());
        }
    }

    private void deployJaegerContent(String fileUrl) throws IOException {
        String yamlContent = TestUtils.setMetadataNamespace(FileUtils.downloadYaml(fileUrl), NAMESPACE)
                .replace("namespace: \"observability\"", "namespace: \"" + NAMESPACE + "\"");
        jaegerConfigs.push(yamlContent);
        cmdKubeClient().applyContent(yamlContent);
    }

    private void deployJaegerOperator() throws IOException {
        LOGGER.info("=== Applying jaeger operator install files ===");

        deployJaegerContent("https://raw.githubusercontent.com/jaegertracing/jaeger-operator/v" + JAEGER_VERSION + "/deploy/service_account.yaml");
        deployJaegerContent("https://raw.githubusercontent.com/jaegertracing/jaeger-operator/v" + JAEGER_VERSION + "/deploy/role.yaml");
        deployJaegerContent("https://raw.githubusercontent.com/jaegertracing/jaeger-operator/v" + JAEGER_VERSION + "/deploy/role_binding.yaml");
        deployJaegerContent("https://raw.githubusercontent.com/jaegertracing/jaeger-operator/v" + JAEGER_VERSION + "/deploy/cluster_role.yaml");
        deployJaegerContent("https://raw.githubusercontent.com/jaegertracing/jaeger-operator/v" + JAEGER_VERSION + "/deploy/cluster_role_binding.yaml");
        deployJaegerContent("https://raw.githubusercontent.com/jaegertracing/jaeger-operator/v" + JAEGER_VERSION + "/deploy/crds/jaegertracing.io_jaegers_crd.yaml");
        deployJaegerContent("https://raw.githubusercontent.com/jaegertracing/jaeger-operator/v" + JAEGER_VERSION + "/deploy/operator.yaml");

        ResourceManager.getPointerResources().push(this::deleteJaeger);
        DeploymentUtils.waitForDeploymentAndPodsReady(JAEGER_OPERATOR_DEPLOYMENT_NAME, 1);

        NetworkPolicy networkPolicy = new NetworkPolicyBuilder()
            .withNewApiVersion("networking.k8s.io/v1")
            .withNewKind("NetworkPolicy")
            .withNewMetadata()
                .withName("jaeger-allow")
            .endMetadata()
            .withNewSpec()
                .addNewIngress()
                .endIngress()
                .withNewPodSelector()
                    .addToMatchLabels("app", "jaeger")
                .endPodSelector()
                .withPolicyTypes("Ingress")
            .endSpec()
            .build();

        LOGGER.debug("Going to apply the following NetworkPolicy: {}", networkPolicy.toString());
        KubernetesResource.deleteLater(kubeClient().getClient().network().networkPolicies().inNamespace(ResourceManager.kubeClient().getNamespace()).createOrReplace(networkPolicy));
        LOGGER.info("Network policy for jaeger successfully applied");
    }

    /**
     * Install of Jaeger instance
     */
    void deployJaegerInstance() {
        LOGGER.info("=== Applying jaeger instance install file ===");

        String instanceYamlContent = TestUtils.getContent(new File(JAEGER_INSTANCE_PATH), TestUtils::toYamlString);
        cmdKubeClient().applyContent(instanceYamlContent.replaceAll("image: 'all-in-one:*'", "image: 'all-in-one:" + JAEGER_VERSION.substring(0, 4) + "'"));
        ResourceManager.getPointerResources().push(() -> cmdKubeClient().deleteContent(instanceYamlContent));
        DeploymentUtils.waitForDeploymentAndPodsReady(JAEGER_INSTANCE_NAME, 1);
    }

    @BeforeEach
    void createTestResources() {
        deployJaegerInstance();
        KafkaClientsResource.createAndWaitForReadiness(KafkaClientsResource.deployKafkaClients(false, kafkaClientsName).build());

        kafkaClientsPodName = kubeClient().listPodsByPrefixInName(kafkaClientsName).get(0).getMetadata().getName();

        kafkaTracingClient = new KafkaTracingExampleClients.Builder()
            .withProducerName(PRODUCER_JOB_NAME)
            .withConsumerName(CONSUMER_JOB_NAME)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(clusterName))
            .withTopicName(TOPIC_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withJaegerServiceProducerName(JAEGER_PRODUCER_SERVICE)
            .withJaegerServiceConsumerName(JAEGER_CONSUMER_SERVICE)
            .withJaegerServiceStreamsName(JAEGER_KAFKA_STREAMS_SERVICE)
            .build();
    }

    @BeforeAll
    void setup() throws IOException {
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE);
        // deployment of the jaeger
        deployJaegerOperator();
    }
}
