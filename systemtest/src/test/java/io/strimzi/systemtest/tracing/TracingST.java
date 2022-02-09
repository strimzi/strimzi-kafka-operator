/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.tracing;

import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyBuilder;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.annotations.ParallelSuite;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.resources.ResourceItem;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBridgeExampleClients;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaTracingExampleClients;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMakerTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.specific.TracingUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
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
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.KAFKA_TRACING_CLIENT_KEY;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER2;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.TRACING;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@Tag(REGRESSION)
@Tag(TRACING)
@Tag(INTERNAL_CLIENTS_USED)
@ParallelSuite
public class TracingST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(TracingST.class);

    private static final String JAEGER_PRODUCER_SERVICE = "hello-world-producer";
    private static final String JAEGER_CONSUMER_SERVICE = "hello-world-consumer";
    private static final String JAEGER_KAFKA_STREAMS_SERVICE = "hello-world-streams";
    private static final String JAEGER_MIRROR_MAKER_SERVICE = "my-mirror-maker";
    private static final String JAEGER_MIRROR_MAKER2_SERVICE = "my-mirror-maker2";
    private static final String JAEGER_KAFKA_CONNECT_SERVICE = "my-connect";
    private static final String JAEGER_KAFKA_BRIDGE_SERVICE = "my-kafka-bridge";

    protected static final String PRODUCER_JOB_NAME = "hello-world-producer";
    protected static final String CONSUMER_JOB_NAME = "hello-world-consumer";

    private static final String JAEGER_INSTANCE_NAME = "my-jaeger";
    private static final String JAEGER_SAMPLER_TYPE = "const";
    private static final String JAEGER_SAMPLER_PARAM = "1";
    private static final String JAEGER_OPERATOR_DEPLOYMENT_NAME = "jaeger-operator";
    private static final String JAEGER_AGENT_NAME = JAEGER_INSTANCE_NAME + "-agent";
    private static final String JAEGER_QUERY_SERVICE = JAEGER_INSTANCE_NAME + "-query";

    private static final String JAEGER_VERSION = "1.22.1";

    private Stack<String> jaegerConfigs = new Stack<>();

    private final String jaegerInstancePath = TestUtils.USER_PATH + "/../systemtest/src/test/resources/tracing/" + TracingUtils.getValidTracingVersion() + "/jaeger-instance.yaml";
    private final String jaegerOperatorFilesPath = TestUtils.USER_PATH + "/../systemtest/src/test/resources/tracing/" + TracingUtils.getValidTracingVersion() + "/operator-files/";

    private final String namespace = testSuiteNamespaceManager.getMapOfAdditionalNamespaces().get(TracingST.class.getSimpleName()).stream().findFirst().get();

    @ParallelNamespaceTest
    @Tag(ACCEPTANCE)
    void testProducerConsumerStreamsService(ExtensionContext extensionContext) {
        // Current implementation of Jaeger deployment and test parallelism does not allow to run this test with STRIMZI_RBAC_SCOPE=NAMESPACE`
        assumeFalse(Environment.isNamespaceRbacScope());

        resourceManager.createResource(extensionContext,
            KafkaTemplates.kafkaEphemeral(
                storageMap.get(extensionContext).getClusterName(), 3, 1).build());

        resourceManager.createResource(extensionContext,
            KafkaTopicTemplates.topic(storageMap.get(extensionContext).getClusterName(),
                storageMap.get(extensionContext).getTopicName())
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext,
            KafkaTopicTemplates.topic(storageMap.get(extensionContext).getClusterName(),
                storageMap.get(extensionContext).retrieveFromTestStorage(Constants.STREAM_TOPIC_KEY).toString())
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, ((KafkaTracingExampleClients) storageMap.get(extensionContext).retrieveFromTestStorage(KAFKA_TRACING_CLIENT_KEY)).producerWithTracing().build());

        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(),
            JAEGER_PRODUCER_SERVICE,
            storageMap.get(extensionContext).retrieveFromTestStorage(Constants.KAFKA_CLIENTS_POD_KEY).toString(),
            JAEGER_QUERY_SERVICE);

        resourceManager.createResource(extensionContext, ((KafkaTracingExampleClients) storageMap.get(extensionContext).retrieveFromTestStorage(KAFKA_TRACING_CLIENT_KEY)).consumerWithTracing().build());

        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(),
            JAEGER_CONSUMER_SERVICE,
            storageMap.get(extensionContext).retrieveFromTestStorage(Constants.KAFKA_CLIENTS_POD_KEY).toString(),
            JAEGER_QUERY_SERVICE);

        resourceManager.createResource(extensionContext, ((KafkaTracingExampleClients) storageMap.get(extensionContext).retrieveFromTestStorage(KAFKA_TRACING_CLIENT_KEY)).kafkaStreamsWithTracing().build());

//        TODO: Disabled because of issue with Streams API and tracing. Uncomment this after fix. https://github.com/strimzi/strimzi-kafka-operator/issues/5680
//        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(),
//            JAEGER_KAFKA_STREAMS_SERVICE,
//            storageMap.get(extensionContext).retrieveFromTestStorage(Constants.KAFKA_CLIENTS_POD_KEY).toString(),
//            JAEGER_QUERY_SERVICE);
    }

    @ParallelNamespaceTest
    @Tag(MIRROR_MAKER2)
    void testProducerConsumerMirrorMaker2Service(ExtensionContext extensionContext) {
        // Current implementation of Jaeger deployment and test parallelism does not allow to run this test with STRIMZI_RBAC_SCOPE=NAMESPACE`
        assumeFalse(Environment.isNamespaceRbacScope());

        final String kafkaClusterSourceName = storageMap.get(extensionContext).getClusterName();
        final String kafkaClusterTargetName = storageMap.get(extensionContext).getClusterName() + "-target";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 3, 1).build());
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 3, 1).build());

        // Create topic and deploy clients before Mirror Maker to not wait for MM to find the new topics
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, storageMap.get(extensionContext).getTopicName())
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterTargetName, kafkaClusterSourceName + "." + storageMap.get(extensionContext).getTopicName())
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        LOGGER.info("Setting for kafka source plain bootstrap:{}", KafkaResources.plainBootstrapAddress(kafkaClusterSourceName));

        KafkaTracingExampleClients sourceKafkaTracingClient = ((KafkaTracingExampleClients) storageMap.get(extensionContext).retrieveFromTestStorage(KAFKA_TRACING_CLIENT_KEY)).toBuilder()
            .withTopicName(storageMap.get(extensionContext).getTopicName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName))
            .build();

        resourceManager.createResource(extensionContext, sourceKafkaTracingClient.producerWithTracing().build());

        LOGGER.info("Setting for kafka target plain bootstrap:{}", KafkaResources.plainBootstrapAddress(kafkaClusterTargetName));

        KafkaTracingExampleClients targetKafkaTracingClient = ((KafkaTracingExampleClients) storageMap.get(extensionContext).retrieveFromTestStorage(KAFKA_TRACING_CLIENT_KEY)).toBuilder()
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))
            .withTopicName(kafkaClusterSourceName + "." + storageMap.get(extensionContext).getTopicName())
            .build();

        resourceManager.createResource(extensionContext, targetKafkaTracingClient.consumerWithTracing().build());

        resourceManager.createResource(extensionContext, KafkaMirrorMaker2Templates.kafkaMirrorMaker2(storageMap.get(extensionContext).getClusterName(), kafkaClusterTargetName, kafkaClusterSourceName, 1, false)
            .editSpec()
                .withNewJaegerTracing()
                .endJaegerTracing()
                .withNewTemplate()
                    .withNewConnectContainer()
                        .addNewEnv()
                            .withName("JAEGER_SERVICE_NAME")
                            .withValue(JAEGER_MIRROR_MAKER2_SERVICE)
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

        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(),
            JAEGER_PRODUCER_SERVICE,
            storageMap.get(extensionContext).retrieveFromTestStorage(Constants.KAFKA_CLIENTS_POD_KEY).toString(),
            "To_" + storageMap.get(extensionContext).getTopicName(),
            JAEGER_QUERY_SERVICE);
        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(),
            JAEGER_CONSUMER_SERVICE, storageMap.get(extensionContext).retrieveFromTestStorage(Constants.KAFKA_CLIENTS_POD_KEY).toString(), "From_" + kafkaClusterSourceName + "." + storageMap.get(extensionContext).getTopicName(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(),
            JAEGER_MIRROR_MAKER2_SERVICE,
            storageMap.get(extensionContext).retrieveFromTestStorage(Constants.KAFKA_CLIENTS_POD_KEY).toString(),
            "From_" + storageMap.get(extensionContext).getTopicName(),
            JAEGER_QUERY_SERVICE);
        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(),
            JAEGER_MIRROR_MAKER2_SERVICE, storageMap.get(extensionContext).retrieveFromTestStorage(Constants.KAFKA_CLIENTS_POD_KEY).toString(),
            "To_" + kafkaClusterSourceName + "." + storageMap.get(extensionContext).getTopicName(), JAEGER_QUERY_SERVICE);
    }

    @ParallelNamespaceTest
    @Tag(MIRROR_MAKER)
    void testProducerConsumerMirrorMakerService(ExtensionContext extensionContext) {
        // Current implementation of Jaeger deployment and test parallelism does not allow to run this test with STRIMZI_RBAC_SCOPE=NAMESPACE`
        assumeFalse(Environment.isNamespaceRbacScope());

        final String kafkaClusterSourceName = storageMap.get(extensionContext).getClusterName();
        final String kafkaClusterTargetName = storageMap.get(extensionContext).getClusterName() + "-target";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 3, 1).build());
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 3, 1).build());

        // Create topic and deploy clients before Mirror Maker to not wait for MM to find the new topics
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, storageMap.get(extensionContext).getTopicName())
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterTargetName, storageMap.get(extensionContext).getTopicName() + "-target")
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
                .withTopicName(storageMap.get(extensionContext).getTopicName())
            .endSpec()
            .build());

        LOGGER.info("Setting for kafka source plain bootstrap:{}", KafkaResources.plainBootstrapAddress(kafkaClusterSourceName));

        KafkaTracingExampleClients sourceKafkaTracingClient = ((KafkaTracingExampleClients) storageMap.get(extensionContext).retrieveFromTestStorage(KAFKA_TRACING_CLIENT_KEY)).toBuilder()
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName))
            .build();

        resourceManager.createResource(extensionContext, sourceKafkaTracingClient.producerWithTracing().build());

        LOGGER.info("Setting for kafka target plain bootstrap:{}", KafkaResources.plainBootstrapAddress(kafkaClusterTargetName));

        KafkaTracingExampleClients targetKafkaTracingClient =  ((KafkaTracingExampleClients) storageMap.get(extensionContext).retrieveFromTestStorage(KAFKA_TRACING_CLIENT_KEY)).toBuilder()
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))
            .build();

        resourceManager.createResource(extensionContext, targetKafkaTracingClient.consumerWithTracing().build());

        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(storageMap.get(extensionContext).getClusterName(), kafkaClusterSourceName, kafkaClusterTargetName,
            ClientUtils.generateRandomConsumerGroup(), 1, false)
                .editSpec()
                    .withNewJaegerTracing()
                    .endJaegerTracing()
                    .withNewTemplate()
                        .withNewMirrorMakerContainer()
                            .addNewEnv()
                                .withName("JAEGER_SERVICE_NAME")
                                .withValue(JAEGER_MIRROR_MAKER_SERVICE)
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
                        .endMirrorMakerContainer()
                    .endTemplate()
                .endSpec()
                .build());

        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(), JAEGER_PRODUCER_SERVICE, storageMap.get(extensionContext).retrieveFromTestStorage(Constants.KAFKA_CLIENTS_POD_KEY).toString(), "To_" + storageMap.get(extensionContext).getTopicName(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(), JAEGER_CONSUMER_SERVICE, storageMap.get(extensionContext).retrieveFromTestStorage(Constants.KAFKA_CLIENTS_POD_KEY).toString(), "From_" + storageMap.get(extensionContext).getTopicName(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(), JAEGER_MIRROR_MAKER_SERVICE, storageMap.get(extensionContext).retrieveFromTestStorage(Constants.KAFKA_CLIENTS_POD_KEY).toString(), "From_" + storageMap.get(extensionContext).getTopicName(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(), JAEGER_MIRROR_MAKER_SERVICE, storageMap.get(extensionContext).retrieveFromTestStorage(Constants.KAFKA_CLIENTS_POD_KEY).toString(), "To_" + storageMap.get(extensionContext).getTopicName(), JAEGER_QUERY_SERVICE);
    }

    @ParallelNamespaceTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testProducerConsumerStreamsConnectService(ExtensionContext extensionContext) {
        // Current implementation of Jaeger deployment and test parallelism does not allow to run this test with STRIMZI_RBAC_SCOPE=NAMESPACE`
        assumeFalse(Environment.isNamespaceRbacScope());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(storageMap.get(extensionContext).getClusterName(), 3, 1).build());

        // Create topic and deploy clients before Mirror Maker to not wait for MM to find the new topics
        resourceManager.createResource(extensionContext,
            KafkaTopicTemplates.topic(storageMap.get(extensionContext).getClusterName(),
                storageMap.get(extensionContext).getTopicName())
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext,
            KafkaTopicTemplates.topic(storageMap.get(extensionContext).getClusterName(),
                    storageMap.get(extensionContext).retrieveFromTestStorage(Constants.STREAM_TOPIC_KEY).toString())
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        Map<String, Object> configOfKafkaConnect = new HashMap<>();
        configOfKafkaConnect.put("config.storage.replication.factor", "-1");
        configOfKafkaConnect.put("offset.storage.replication.factor", "-1");
        configOfKafkaConnect.put("status.storage.replication.factor", "-1");
        configOfKafkaConnect.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("key.converter.schemas.enable", "false");
        configOfKafkaConnect.put("value.converter.schemas.enable", "false");

        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, storageMap.get(extensionContext).getClusterName(), 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .withNewSpec()
                .withConfig(configOfKafkaConnect)
                .withNewJaegerTracing()
                .endJaegerTracing()
                .withBootstrapServers(KafkaResources.plainBootstrapAddress(storageMap.get(extensionContext).getClusterName()))
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

        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(storageMap.get(extensionContext).getClusterName())
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("file", Constants.DEFAULT_SINK_FILE_PATH)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("topics", storageMap.get(extensionContext).getTopicName())
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, ((KafkaTracingExampleClients) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_TRACING_CLIENT_KEY)).producerWithTracing().build());
        resourceManager.createResource(extensionContext, ((KafkaTracingExampleClients) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_TRACING_CLIENT_KEY)).consumerWithTracing().build());

        ClientUtils.waitTillContinuousClientsFinish(
            storageMap.get(extensionContext).getProducerName(),
            storageMap.get(extensionContext).getConsumerName(),
            storageMap.get(extensionContext).getNamespaceName(), MESSAGE_COUNT);

        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(), JAEGER_PRODUCER_SERVICE, storageMap.get(extensionContext).retrieveFromTestStorage(Constants.KAFKA_CLIENTS_POD_KEY).toString(), "To_" + storageMap.get(extensionContext).getTopicName(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(), JAEGER_CONSUMER_SERVICE, storageMap.get(extensionContext).retrieveFromTestStorage(Constants.KAFKA_CLIENTS_POD_KEY).toString(), "From_" + storageMap.get(extensionContext).getTopicName(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(), JAEGER_KAFKA_CONNECT_SERVICE, storageMap.get(extensionContext).retrieveFromTestStorage(Constants.KAFKA_CLIENTS_POD_KEY).toString(), "From_" + storageMap.get(extensionContext).getTopicName(), JAEGER_QUERY_SERVICE);
        // TODO: Disabled because of issue with Streams API and tracing. Uncomment this after fix. https://github.com/strimzi/strimzi-kafka-operator/issues/5680
//        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(), JAEGER_KAFKA_STREAMS_SERVICE, storageMap.get(extensionContext).retrieveFromTestStorage(Constants.KAFKA_CLIENTS_POD_KEY).toString(), "From_" + storageMap.get(extensionContext).getTopicName(), JAEGER_QUERY_SERVICE);
//        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(), JAEGER_KAFKA_STREAMS_SERVICE, storageMap.get(extensionContext).retrieveFromTestStorage(Constants.KAFKA_CLIENTS_POD_KEY).toString(), "To_" + storageMap.get(extensionContext).retrieveFromTestStorage(Constants.STREAM_TOPIC_KEY).toString(), JAEGER_QUERY_SERVICE);
    }

    @Tag(BRIDGE)
    @ParallelNamespaceTest
    void testKafkaBridgeService(ExtensionContext extensionContext) {
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(storageMap.get(extensionContext).getClusterName(), 3, 1).build());
        // Deploy http bridge
        resourceManager.createResource(extensionContext, KafkaBridgeTemplates.kafkaBridge(storageMap.get(extensionContext).getClusterName(), KafkaResources.plainBootstrapAddress(storageMap.get(extensionContext).getClusterName()), 1)
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
        resourceManager.createResource(extensionContext,
            KafkaTopicTemplates.topic(storageMap.get(extensionContext).getClusterName(), storageMap.get(extensionContext).getTopicName())
                .build());

        KafkaBridgeExampleClients kafkaBridgeClientJob = new KafkaBridgeExampleClients.Builder()
            .withProducerName(bridgeProducer)
            .withNamespaceName(storageMap.get(extensionContext).getNamespaceName())
            .withBootstrapAddress(KafkaBridgeResources.serviceName(storageMap.get(extensionContext).getClusterName()))
            .withTopicName(storageMap.get(extensionContext).getTopicName())
            .withMessageCount(MESSAGE_COUNT)
            .withPort(Constants.HTTP_BRIDGE_DEFAULT_PORT)
            .withDelayMs(1000)
            .withPollInterval(1000)
            .build();

        resourceManager.createResource(extensionContext, kafkaBridgeClientJob.producerStrimziBridge().build());
        resourceManager.createResource(extensionContext, ((KafkaTracingExampleClients) storageMap.get(extensionContext).retrieveFromTestStorage(KAFKA_TRACING_CLIENT_KEY)).consumerWithTracing().build());
        ClientUtils.waitForClientSuccess(bridgeProducer, storageMap.get(extensionContext).getNamespaceName(), MESSAGE_COUNT);

        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(), JAEGER_KAFKA_BRIDGE_SERVICE, storageMap.get(extensionContext).retrieveFromTestStorage(Constants.KAFKA_CLIENTS_POD_KEY).toString(), JAEGER_QUERY_SERVICE);
    }

    /**
     * Delete Jaeger instance
     */
    void deleteJaeger() {
        while (!jaegerConfigs.empty()) {
            cmdKubeClient().namespace(namespace).deleteContent(jaegerConfigs.pop());
        }
    }

    private void deployJaegerContent(ExtensionContext extensionContext) throws FileNotFoundException {
        File folder = new File(jaegerOperatorFilesPath);
        File[] files = folder.listFiles();

        if (files != null && files.length > 0) {
            for (File file : files) {
                String yamlContent = TestUtils.setMetadataNamespace(file, namespace)
                    .replace("namespace: \"observability\"", "namespace: \"" + namespace + "\"");
                jaegerConfigs.push(yamlContent);
                LOGGER.info("Creating {} from {}", file.getName(), file.getAbsolutePath());
                cmdKubeClient(namespace).applyContent(yamlContent);
            }
        } else {
            throw new FileNotFoundException("Folder with Jaeger files is empty or doesn't exist");
        }
    }

    private void deployJaegerOperator(ExtensionContext extensionContext) throws IOException, FileNotFoundException {
        LOGGER.info("=== Applying jaeger operator install files ===");

        deployJaegerContent(extensionContext);

        ResourceManager.STORED_RESOURCES.computeIfAbsent(extensionContext.getDisplayName(), k -> new Stack<>());
        ResourceManager.STORED_RESOURCES.get(extensionContext.getDisplayName()).push(new ResourceItem(() -> this.deleteJaeger()));
        DeploymentUtils.waitForDeploymentAndPodsReady(namespace, JAEGER_OPERATOR_DEPLOYMENT_NAME, 1);

        NetworkPolicy networkPolicy = new NetworkPolicyBuilder()
            .withApiVersion("networking.k8s.io/v1")
            .withKind(Constants.NETWORK_POLICY)
            .withNewMetadata()
                .withName("jaeger-allow")
                .withNamespace(namespace)
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

        LOGGER.debug("Creating NetworkPolicy: {}", networkPolicy.toString());
        resourceManager.createResource(extensionContext, networkPolicy);
        LOGGER.info("Network policy for jaeger successfully created");
    }

    /**
     * Install of Jaeger instance
     */
    void deployJaegerInstance(ExtensionContext extensionContext, String namespaceName) {
        LOGGER.info("=== Applying jaeger instance install file ===");

        String instanceYamlContent = TestUtils.getContent(new File(jaegerInstancePath), TestUtils::toYamlString);
        cmdKubeClient(namespaceName).applyContent(instanceYamlContent.replaceAll("image: 'jaegertracing/all-in-one:*'", "image: 'jaegertracing/all-in-one:" + JAEGER_VERSION.substring(0, 4) + "'"));
        ResourceManager.STORED_RESOURCES.computeIfAbsent(extensionContext.getDisplayName(), k -> new Stack<>());
        ResourceManager.STORED_RESOURCES.get(extensionContext.getDisplayName()).push(new ResourceItem(() -> cmdKubeClient(namespaceName).deleteContent(instanceYamlContent)));
        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, JAEGER_INSTANCE_NAME, 1);
    }

    @BeforeEach
    void createTestResources(ExtensionContext extensionContext) {
        TestStorage testStorage = new TestStorage(extensionContext, namespace);

        storageMap.put(extensionContext, testStorage);

        deployJaegerInstance(extensionContext, storageMap.get(extensionContext).getNamespaceName());

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(storageMap.get(extensionContext).getNamespaceName(), false, storageMap.get(extensionContext).getKafkaClientsName()).build());

        testStorage.addToTestStorage(Constants.KAFKA_CLIENTS_POD_KEY, kubeClient(storageMap.get(extensionContext).getNamespaceName()).listPodsByPrefixInName(storageMap.get(extensionContext).getKafkaClientsName()).get(0).getMetadata().getName());

        storageMap.put(extensionContext, testStorage);

        final KafkaTracingExampleClients kafkaTracingClient = new KafkaTracingExampleClients.Builder()
            .withNamespaceName(storageMap.get(extensionContext).getNamespaceName())
            .withProducerName(storageMap.get(extensionContext).getProducerName())
            .withConsumerName(storageMap.get(extensionContext).getConsumerName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(storageMap.get(extensionContext).getClusterName()))
            .withTopicName(storageMap.get(extensionContext).getTopicName())
            .withStreamsTopicTargetName(storageMap.get(extensionContext).retrieveFromTestStorage(Constants.STREAM_TOPIC_KEY).toString())
            .withMessageCount(MESSAGE_COUNT)
            .withJaegerServiceProducerName(JAEGER_PRODUCER_SERVICE)
            .withJaegerServiceConsumerName(JAEGER_CONSUMER_SERVICE)
            .withJaegerServiceStreamsName(JAEGER_KAFKA_STREAMS_SERVICE)
            .withJaegerServiceAgentName(JAEGER_AGENT_NAME)
            .build();

        testStorage.addToTestStorage(Constants.KAFKA_TRACING_CLIENT_KEY, kafkaTracingClient);

        storageMap.put(extensionContext, testStorage);
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) throws IOException {
        // deployment of the jaeger
        deployJaegerOperator(extensionContext);
    }
}
