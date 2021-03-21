/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.tracing;

import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyBuilder;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBridgeExampleClients;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaTracingExampleClients;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectS2ITemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMakerTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.specific.TracingUtils;
import io.strimzi.test.TestUtils;
import io.vertx.junit5.VertxExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.FileNotFoundException;
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
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@Tag(REGRESSION)
@Tag(TRACING)
@Tag(INTERNAL_CLIENTS_USED)
@ExtendWith(VertxExtension.class)
@Disabled
public class TracingST extends AbstractST {

    private static final String NAMESPACE = "tracing-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(TracingST.class);

    private static final String JAEGER_INSTANCE_PATH = TestUtils.USER_PATH + "/../systemtest/src/test/resources/tracing/jaeger-instance.yaml";
    private static final String JAEGER_OPERATOR_FILES_PATH = TestUtils.USER_PATH + "/../systemtest/src/test/resources/tracing/operator-files/";

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

    private static final String JAEGER_INSTANCE_NAME = "my-jaeger";
    private static final String JAEGER_SAMPLER_TYPE = "const";
    private static final String JAEGER_SAMPLER_PARAM = "1";
    private static final String JAEGER_OPERATOR_DEPLOYMENT_NAME = "jaeger-operator";
    private static final String JAEGER_AGENT_NAME = JAEGER_INSTANCE_NAME + "-agent";
    private static final String JAEGER_QUERY_SERVICE = JAEGER_INSTANCE_NAME + "-query";

    private static final String JAEGER_VERSION = "1.18.1";

    private static final String CLUSTER_NAME = "tracing-cluster";

    private Stack<String> jaegerConfigs = new Stack<>();

    private String kafkaClientsPodName;

    private static KafkaTracingExampleClients kafkaTracingClient;

    private String topicName = "";
    private String streamsTopicTargetName = "";
    private String kafkaClientsName = NAMESPACE + "-shared-kafka-clients";

    @IsolatedTest
    void testProducerService(ExtensionContext extensionContext) {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(CLUSTER_NAME, topicName)
            .editSpec()
                .withReplicas(1)
                .withPartitions(12)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, kafkaTracingClient.producerWithTracing().build());

        TracingUtils.verify(JAEGER_PRODUCER_SERVICE, kafkaClientsPodName, JAEGER_QUERY_SERVICE);
    }

    @IsolatedTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testConnectService(ExtensionContext extensionContext) {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        Map<String, Object> configOfKafkaConnect = new HashMap<>();
        configOfKafkaConnect.put("config.storage.replication.factor", "1");
        configOfKafkaConnect.put("offset.storage.replication.factor", "1");
        configOfKafkaConnect.put("status.storage.replication.factor", "1");
        configOfKafkaConnect.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("key.converter.schemas.enable", "false");
        configOfKafkaConnect.put("value.converter.schemas.enable", "false");

        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, CLUSTER_NAME, 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .withNewSpec()
                .withConfig(configOfKafkaConnect)
                .withNewJaegerTracing()
                .endJaegerTracing()
                .withBootstrapServers(KafkaResources.plainBootstrapAddress(CLUSTER_NAME))
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

        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(CLUSTER_NAME)
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("file", Constants.DEFAULT_SINK_FILE_PATH)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("topics", topicName)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, kafkaTracingClient.producerWithTracing().build());
        resourceManager.createResource(extensionContext, kafkaTracingClient.consumerWithTracing().build());

        TracingUtils.verify(JAEGER_KAFKA_CONNECT_SERVICE, kafkaClientsPodName, JAEGER_QUERY_SERVICE);
    }

    @IsolatedTest
    void testProducerWithStreamsService(ExtensionContext extensionContext) {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());
        String targetTopicName = KafkaTopicUtils.generateRandomNameOfTopic();

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(CLUSTER_NAME, topicName)
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(CLUSTER_NAME, targetTopicName)
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, kafkaTracingClient.producerWithTracing().build());

        TracingUtils.verify(JAEGER_PRODUCER_SERVICE, kafkaClientsPodName, JAEGER_QUERY_SERVICE);

        resourceManager.createResource(extensionContext, kafkaTracingClient.kafkaStreamsWithTracing().build());

        TracingUtils.verify(JAEGER_KAFKA_STREAMS_SERVICE, kafkaClientsPodName, JAEGER_QUERY_SERVICE);
    }

    @IsolatedTest
    void testProducerConsumerService(ExtensionContext extensionContext) {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(CLUSTER_NAME, topicName)
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, kafkaTracingClient.producerWithTracing().build());

        TracingUtils.verify(JAEGER_PRODUCER_SERVICE, kafkaClientsPodName, JAEGER_QUERY_SERVICE);

        resourceManager.createResource(extensionContext, kafkaTracingClient.consumerWithTracing().build());

        TracingUtils.verify(JAEGER_CONSUMER_SERVICE, kafkaClientsPodName, JAEGER_QUERY_SERVICE);
    }

    @IsolatedTest
    @Tag(ACCEPTANCE)
    void testProducerConsumerStreamsService(ExtensionContext extensionContext) {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(CLUSTER_NAME, topicName)
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(CLUSTER_NAME, streamsTopicTargetName)
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, kafkaTracingClient.producerWithTracing().build());

        TracingUtils.verify(JAEGER_PRODUCER_SERVICE, kafkaClientsPodName, JAEGER_QUERY_SERVICE);

        resourceManager.createResource(extensionContext, kafkaTracingClient.consumerWithTracing().build());

        TracingUtils.verify(JAEGER_CONSUMER_SERVICE, kafkaClientsPodName, JAEGER_QUERY_SERVICE);

        resourceManager.createResource(extensionContext, kafkaTracingClient.kafkaStreamsWithTracing().build());

        TracingUtils.verify(JAEGER_KAFKA_STREAMS_SERVICE, kafkaClientsPodName, JAEGER_QUERY_SERVICE);
    }

    @IsolatedTest
    @Tag(MIRROR_MAKER2)
    void testProducerConsumerMirrorMaker2Service(ExtensionContext extensionContext) {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        final String kafkaClusterSourceName = CLUSTER_NAME;
        final String kafkaClusterTargetName = CLUSTER_NAME + "-target";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 3, 1)
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
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, topicName)
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterTargetName, kafkaClusterSourceName + "." + topicName)
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        LOGGER.info("Setting for kafka source plain bootstrap:{}", KafkaResources.plainBootstrapAddress(kafkaClusterSourceName));

        KafkaTracingExampleClients sourceKafkaTracingClient = kafkaTracingClient.toBuilder()
            .withTopicName(topicName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName))
            .build();

        resourceManager.createResource(extensionContext, sourceKafkaTracingClient.producerWithTracing().build());

        LOGGER.info("Setting for kafka target plain bootstrap:{}", KafkaResources.plainBootstrapAddress(kafkaClusterTargetName));

        KafkaTracingExampleClients targetKafkaTracingClient = kafkaTracingClient.toBuilder()
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))
            .withTopicName(kafkaClusterSourceName + "." + topicName)
            .build();

        resourceManager.createResource(extensionContext, targetKafkaTracingClient.consumerWithTracing().build());

        resourceManager.createResource(extensionContext, KafkaMirrorMaker2Templates.kafkaMirrorMaker2(CLUSTER_NAME, kafkaClusterTargetName, kafkaClusterSourceName, 1, false)
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

        TracingUtils.verify(JAEGER_PRODUCER_SERVICE, kafkaClientsPodName, "To_" + topicName, JAEGER_QUERY_SERVICE);
        TracingUtils.verify(JAEGER_CONSUMER_SERVICE, kafkaClientsPodName, "From_" + kafkaClusterSourceName + "." + topicName, JAEGER_QUERY_SERVICE);
        TracingUtils.verify(JAEGER_MIRROR_MAKER2_SERVICE, kafkaClientsPodName, "From_" + topicName, JAEGER_QUERY_SERVICE);
        TracingUtils.verify(JAEGER_MIRROR_MAKER2_SERVICE, kafkaClientsPodName, "To_" + kafkaClusterSourceName + "." + topicName, JAEGER_QUERY_SERVICE);
    }

    @IsolatedTest
    @Tag(MIRROR_MAKER)
    void testProducerConsumerMirrorMakerService(ExtensionContext extensionContext) {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        final String kafkaClusterSourceName = CLUSTER_NAME;
        final String kafkaClusterTargetName = CLUSTER_NAME + "-target";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 3, 1)
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
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, topicName)
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterTargetName, topicName + "-target")
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
                .withTopicName(topicName)
            .endSpec()
            .build());

        LOGGER.info("Setting for kafka source plain bootstrap:{}", KafkaResources.plainBootstrapAddress(kafkaClusterSourceName));

        KafkaTracingExampleClients sourceKafkaTracingClient = kafkaTracingClient.toBuilder()
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName))
            .build();

        resourceManager.createResource(extensionContext, sourceKafkaTracingClient.producerWithTracing().build());

        LOGGER.info("Setting for kafka target plain bootstrap:{}", KafkaResources.plainBootstrapAddress(kafkaClusterTargetName));

        KafkaTracingExampleClients targetKafkaTracingClient =  kafkaTracingClient.toBuilder()
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))
            .build();

        resourceManager.createResource(extensionContext, targetKafkaTracingClient.consumerWithTracing().build());

        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(CLUSTER_NAME, kafkaClusterSourceName, kafkaClusterTargetName,
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

        TracingUtils.verify(JAEGER_PRODUCER_SERVICE, kafkaClientsPodName, "To_" + topicName, JAEGER_QUERY_SERVICE);
        TracingUtils.verify(JAEGER_CONSUMER_SERVICE, kafkaClientsPodName, "From_" + topicName, JAEGER_QUERY_SERVICE);
        TracingUtils.verify(JAEGER_MIRROR_MAKER_SERVICE, kafkaClientsPodName, "From_" + topicName, JAEGER_QUERY_SERVICE);
        TracingUtils.verify(JAEGER_MIRROR_MAKER_SERVICE, kafkaClientsPodName, "To_" + topicName, JAEGER_QUERY_SERVICE);
    }

    @IsolatedTest
    @Tag(CONNECT)
    @Tag(MIRROR_MAKER)
    @Tag(CONNECT_COMPONENTS)
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testProducerConsumerMirrorMakerConnectStreamsService(ExtensionContext extensionContext) {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        final String kafkaClusterSourceName = CLUSTER_NAME;
        final String kafkaClusterTargetName = CLUSTER_NAME + "-target";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 3, 1).build());

        // Create topic and deploy clients before Mirror Maker to not wait for MM to find the new topics
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, topicName)
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, streamsTopicTargetName)
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterTargetName,  topicName + "-target")
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
                .withTopicName(topicName)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterTargetName, streamsTopicTargetName + "-target")
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
                .withTopicName(streamsTopicTargetName)
            .endSpec()
            .build());

        KafkaTracingExampleClients sourceKafkaTracingClient = kafkaTracingClient.toBuilder()
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName))
            .build();

        resourceManager.createResource(extensionContext, sourceKafkaTracingClient.producerWithTracing().build());

        KafkaTracingExampleClients targetKafkaTracingClient = kafkaTracingClient.toBuilder()
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))
            .build();

        resourceManager.createResource(extensionContext, targetKafkaTracingClient.consumerWithTracing().build());
        resourceManager.createResource(extensionContext, sourceKafkaTracingClient.kafkaStreamsWithTracing().build());

        Map<String, Object> configOfKafkaConnect = new HashMap<>();
        configOfKafkaConnect.put("config.storage.replication.factor", "1");
        configOfKafkaConnect.put("offset.storage.replication.factor", "1");
        configOfKafkaConnect.put("status.storage.replication.factor", "1");
        configOfKafkaConnect.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("key.converter.schemas.enable", "false");
        configOfKafkaConnect.put("value.converter.schemas.enable", "false");

        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, CLUSTER_NAME, 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
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

        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(CLUSTER_NAME)
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("file", Constants.DEFAULT_SINK_FILE_PATH)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("topics", topicName)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(CLUSTER_NAME, kafkaClusterSourceName, kafkaClusterTargetName,
            ClientUtils.generateRandomConsumerGroup(), 1, false)
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

        kafkaTracingClient = kafkaTracingClient.toBuilder()
            .withProducerName(PRODUCER_JOB_NAME + "-x")
            .withConsumerName(CONSUMER_JOB_NAME + "-x")
            .withTopicName(streamsTopicTargetName)
            .build();

        resourceManager.createResource(extensionContext, kafkaTracingClient.producerStrimzi().build());
        resourceManager.createResource(extensionContext, kafkaTracingClient.consumerStrimzi().build());

        TracingUtils.verify(JAEGER_PRODUCER_SERVICE, kafkaClientsPodName, "To_" + topicName, JAEGER_QUERY_SERVICE);
        TracingUtils.verify(JAEGER_CONSUMER_SERVICE, kafkaClientsPodName, "From_" + topicName, JAEGER_QUERY_SERVICE);
        TracingUtils.verify(JAEGER_KAFKA_CONNECT_SERVICE, kafkaClientsPodName, "From_" + topicName, JAEGER_QUERY_SERVICE);
        TracingUtils.verify(JAEGER_KAFKA_STREAMS_SERVICE, kafkaClientsPodName, "From_" + topicName, JAEGER_QUERY_SERVICE);
        TracingUtils.verify(JAEGER_KAFKA_STREAMS_SERVICE, kafkaClientsPodName, "To_" + streamsTopicTargetName, JAEGER_QUERY_SERVICE);
        TracingUtils.verify(JAEGER_MIRROR_MAKER_SERVICE, kafkaClientsPodName, "From_" + topicName, JAEGER_QUERY_SERVICE);
        TracingUtils.verify(JAEGER_MIRROR_MAKER_SERVICE, kafkaClientsPodName, "To_" + topicName, JAEGER_QUERY_SERVICE);
        TracingUtils.verify(JAEGER_MIRROR_MAKER_SERVICE, kafkaClientsPodName, "From_" + streamsTopicTargetName, JAEGER_QUERY_SERVICE);
        TracingUtils.verify(JAEGER_MIRROR_MAKER_SERVICE, kafkaClientsPodName, "To_" + streamsTopicTargetName, JAEGER_QUERY_SERVICE);
    }

    @IsolatedTest
    @OpenShiftOnly
    @Tag(CONNECT_S2I)
    @Tag(CONNECT_COMPONENTS)
    void testConnectS2IService(ExtensionContext extensionContext) {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        final String kafkaConnectS2IName = "kafka-connect-s2i-name-1";

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, kafkaClientsName).build());

        Map<String, Object> configOfKafkaConnectS2I = new HashMap<>();
        configOfKafkaConnectS2I.put("key.converter.schemas.enable", "false");
        configOfKafkaConnectS2I.put("value.converter.schemas.enable", "false");
        configOfKafkaConnectS2I.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnectS2I.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");

        resourceManager.createResource(extensionContext, KafkaConnectS2ITemplates.kafkaConnectS2I(extensionContext, kafkaConnectS2IName, CLUSTER_NAME, 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
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

        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(kafkaConnectS2IName)
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("file", Constants.DEFAULT_SINK_FILE_PATH)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("topics", topicName)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, kafkaTracingClient.producerWithTracing().build());
        resourceManager.createResource(extensionContext, kafkaTracingClient.consumerWithTracing().build());

        String kafkaConnectS2IPodName = kubeClient().listPods(Labels.STRIMZI_KIND_LABEL, KafkaConnectS2I.RESOURCE_KIND).get(0).getMetadata().getName();
        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(kafkaConnectS2IPodName, Constants.DEFAULT_SINK_FILE_PATH, "99");

        TracingUtils.verify(JAEGER_PRODUCER_SERVICE, kafkaClientsPodName, JAEGER_QUERY_SERVICE);
        TracingUtils.verify(JAEGER_CONSUMER_SERVICE, kafkaClientsPodName, JAEGER_QUERY_SERVICE);
        TracingUtils.verify(JAEGER_KAFKA_CONNECT_S2I_SERVICE, kafkaClientsPodName, JAEGER_QUERY_SERVICE);
    }

    @Tag(NODEPORT_SUPPORTED)
    @Tag(BRIDGE)
    @IsolatedTest
    void testKafkaBridgeService(ExtensionContext extensionContext) {
        // Deploy http bridge
        resourceManager.createResource(extensionContext, KafkaBridgeTemplates.kafkaBridge(CLUSTER_NAME, KafkaResources.plainBootstrapAddress(CLUSTER_NAME), 1)
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
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(CLUSTER_NAME, topicName).build());

        KafkaBridgeExampleClients kafkaBridgeClientJob = new KafkaBridgeExampleClients.Builder()
            .withProducerName(bridgeProducer)
            .withBootstrapAddress(KafkaBridgeResources.serviceName(CLUSTER_NAME))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withPort(bridgePort)
            .withDelayMs(1000)
            .withPollInterval(1000)
            .build();

        resourceManager.createResource(extensionContext, kafkaBridgeClientJob.producerStrimziBridge().build());
        resourceManager.createResource(extensionContext, kafkaTracingClient.consumerWithTracing().build());
        ClientUtils.waitForClientSuccess(bridgeProducer, NAMESPACE, MESSAGE_COUNT);

        TracingUtils.verify(JAEGER_KAFKA_BRIDGE_SERVICE, kafkaClientsPodName, JAEGER_QUERY_SERVICE);
    }

    /**
     * Delete Jaeger instance
     */
    void deleteJaeger() {
        while (!jaegerConfigs.empty()) {
            cmdKubeClient().namespace(cluster.getNamespace()).deleteContent(jaegerConfigs.pop());
        }
    }

    private void deployJaegerContent() throws FileNotFoundException {
        File folder = new File(JAEGER_OPERATOR_FILES_PATH);
        File[] files = folder.listFiles();

        if (files != null && files.length > 0) {
            for (File file : files) {
                String yamlContent = TestUtils.setMetadataNamespace(file, NAMESPACE)
                    .replace("namespace: \"observability\"", "namespace: \"" + NAMESPACE + "\"");
                jaegerConfigs.push(yamlContent);
                LOGGER.info("Creating {} from {}", file.getName(), file.getAbsolutePath());
                cmdKubeClient().applyContent(yamlContent);
            }
        } else {
            throw new FileNotFoundException("Folder with Jaeger files is empty or doesn't exist");
        }
    }

    private void deployJaegerOperator(ExtensionContext extensionContext) throws IOException, FileNotFoundException {
        LOGGER.info("=== Applying jaeger operator install files ===");

        deployJaegerContent();

        ResourceManager.STORED_RESOURCES.computeIfAbsent(extensionContext.getDisplayName(), k -> new Stack<>());
        ResourceManager.STORED_RESOURCES.get(extensionContext.getDisplayName()).push(() -> this.deleteJaeger());
        DeploymentUtils.waitForDeploymentAndPodsReady(JAEGER_OPERATOR_DEPLOYMENT_NAME, 1);

        NetworkPolicy networkPolicy = new NetworkPolicyBuilder()
            .withNewApiVersion("networking.k8s.io/v1")
            .withNewKind(Constants.NETWORK_POLICY)
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
        resourceManager.createResource(extensionContext, networkPolicy);
        LOGGER.info("Network policy for jaeger successfully applied");
    }

    /**
     * Install of Jaeger instance
     */
    void deployJaegerInstance(ExtensionContext extensionContext) {
        LOGGER.info("=== Applying jaeger instance install file ===");

        String instanceYamlContent = TestUtils.getContent(new File(JAEGER_INSTANCE_PATH), TestUtils::toYamlString);
        cmdKubeClient().applyContent(instanceYamlContent.replaceAll("image: 'all-in-one:*'", "image: 'all-in-one:" + JAEGER_VERSION.substring(0, 4) + "'"));
        ResourceManager.STORED_RESOURCES.computeIfAbsent(extensionContext.getDisplayName(), k -> new Stack<>());
        ResourceManager.STORED_RESOURCES.get(extensionContext.getDisplayName()).push(() -> cmdKubeClient().deleteContent(instanceYamlContent));
        DeploymentUtils.waitForDeploymentAndPodsReady(JAEGER_INSTANCE_NAME, 1);
    }

    @BeforeEach
    void createTestResources(ExtensionContext extensionContext) {
        topicName = KafkaTopicUtils.generateRandomNameOfTopic();
        streamsTopicTargetName = KafkaTopicUtils.generateRandomNameOfTopic();

        deployJaegerInstance(extensionContext);

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, kafkaClientsName).build());

        kafkaClientsPodName = kubeClient().listPodsByPrefixInName(kafkaClientsName).get(0).getMetadata().getName();

        kafkaTracingClient = new KafkaTracingExampleClients.Builder()
            .withProducerName(PRODUCER_JOB_NAME)
            .withConsumerName(CONSUMER_JOB_NAME)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(CLUSTER_NAME))
            .withTopicName(topicName)
            .withStreamsTopicTargetName(streamsTopicTargetName)
            .withMessageCount(MESSAGE_COUNT)
            .withJaegerServiceProducerName(JAEGER_PRODUCER_SERVICE)
            .withJaegerServiceConsumerName(JAEGER_CONSUMER_SERVICE)
            .withJaegerServiceStreamsName(JAEGER_KAFKA_STREAMS_SERVICE)
            .withJaegerServiceAgentName(JAEGER_AGENT_NAME)
            .build();
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) throws IOException {
        installClusterOperator(extensionContext, NAMESPACE);
        // deployment of the jaeger
        deployJaegerOperator(extensionContext);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(CLUSTER_NAME, 3, 1)
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
    }
}
