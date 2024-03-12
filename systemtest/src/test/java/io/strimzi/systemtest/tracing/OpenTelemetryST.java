/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.tracing;

import io.strimzi.api.kafka.model.bridge.KafkaBridgeResources;
import io.strimzi.api.kafka.model.common.tracing.OpenTelemetryTracing;
import io.strimzi.api.kafka.model.common.tracing.Tracing;
import io.strimzi.api.kafka.model.connect.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeTracingClients;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeTracingClientsBuilder;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaTracingClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaTracingClientsBuilder;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.jaeger.SetupJaeger;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMakerTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.specific.TracingUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import static io.strimzi.systemtest.TestConstants.ACCEPTANCE;
import static io.strimzi.systemtest.TestConstants.BRIDGE;
import static io.strimzi.systemtest.TestConstants.CONNECT;
import static io.strimzi.systemtest.TestConstants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.TestConstants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.TestConstants.KAFKA_TRACING_CLIENT_KEY;
import static io.strimzi.systemtest.TestConstants.MIRROR_MAKER;
import static io.strimzi.systemtest.TestConstants.MIRROR_MAKER2;
import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static io.strimzi.systemtest.TestConstants.TRACING;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_COLLECTOR_OTLP_URL;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_CONSUMER_SERVICE;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_KAFKA_BRIDGE_SERVICE;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_KAFKA_CONNECT_SERVICE;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_KAFKA_STREAMS_SERVICE;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_MIRROR_MAKER2_SERVICE;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_MIRROR_MAKER_SERVICE;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_PRODUCER_SERVICE;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_QUERY_SERVICE;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@Tag(REGRESSION)
@Tag(TRACING)
@Tag(INTERNAL_CLIENTS_USED)
public class OpenTelemetryST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(OpenTelemetryST.class);

    private final Tracing otelTracing = new OpenTelemetryTracing();

    @ParallelNamespaceTest
    @Tag(ACCEPTANCE)
    void testProducerConsumerStreamsService() {
        // Current implementation of Jaeger deployment and test parallelism does not allow to run this test with STRIMZI_RBAC_SCOPE=NAMESPACE`
        assumeFalse(Environment.isNamespaceRbacScope());

        // This testStorage gets resources set by BeforeEach, so it must be retrieved from storageMap
        final TestStorage testStorage = storageMap.get(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3, 1).build());

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), 12, 3, testStorage.getNamespaceName()).build());

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getStreamsTopicTargetName(), 12, 3, testStorage.getNamespaceName()).build());

        resourceManager.createResourceWithWait((testStorage.getTracingClients()).producerWithTracing());

        TracingUtils.verify(testStorage.getNamespaceName(),
            JAEGER_PRODUCER_SERVICE,
            testStorage.getScraperPodName(),
            JAEGER_QUERY_SERVICE);

        resourceManager.createResourceWithWait((testStorage.getTracingClients()).consumerWithTracing());

        TracingUtils.verify(testStorage.getNamespaceName(),
            JAEGER_CONSUMER_SERVICE,
            testStorage.getScraperPodName(),
            JAEGER_QUERY_SERVICE);

        resourceManager.createResourceWithWait((testStorage.getTracingClients()).kafkaStreamsWithTracing());

        TracingUtils.verify(testStorage.getNamespaceName(),
            JAEGER_KAFKA_STREAMS_SERVICE,
            testStorage.getScraperPodName(),
            JAEGER_QUERY_SERVICE);
    }

    @ParallelNamespaceTest
    @Tag(MIRROR_MAKER2)
    void testProducerConsumerMirrorMaker2Service() {
        // Current implementation of Jaeger deployment and test parallelism does not allow to run this test with STRIMZI_RBAC_SCOPE=NAMESPACE`
        assumeFalse(Environment.isNamespaceRbacScope());

        // This testStorage gets resources set by BeforeEach, so it must be retrieved from storageMap
        final TestStorage testStorage = storageMap.get(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getSourceBrokerPoolName(), testStorage.getSourceClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getSourceControllerPoolName(), testStorage.getSourceClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getSourceClusterName(), 1).build());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getTargetBrokerPoolName(), testStorage.getTargetClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getTargetControllerPoolName(), testStorage.getTargetClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getTargetClusterName(), 1).build());

        // Create topic and deploy clients before Mirror Maker to not wait for MM to find the new topics
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getSourceClusterName(), testStorage.getTopicName(), 12, 3, testStorage.getNamespaceName()).build());

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getTargetClusterName(), testStorage.getMirroredSourceTopicName(), 12, 3, testStorage.getNamespaceName()).build());

        LOGGER.info("Setting for Kafka source plain bootstrap: {}", KafkaResources.plainBootstrapAddress(testStorage.getSourceClusterName()));

        KafkaTracingClients sourceKafkaTracingClient = new KafkaTracingClientsBuilder(testStorage.getTracingClients())
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getSourceClusterName()))
            .build();

        resourceManager.createResourceWithWait(sourceKafkaTracingClient.producerWithTracing());

        LOGGER.info("Setting for Kafka target plain bootstrap: {}", KafkaResources.plainBootstrapAddress(testStorage.getTargetClusterName()));

        final KafkaTracingClients targetKafkaTracingClient = new KafkaTracingClientsBuilder(testStorage.getTracingClients())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getTargetClusterName()))
            .withTopicName(testStorage.getMirroredSourceTopicName())
            .build();

        resourceManager.createResourceWithWait(targetKafkaTracingClient.consumerWithTracing());

        resourceManager.createResourceWithWait(KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage.getClusterName(), testStorage.getTargetClusterName(), testStorage.getSourceClusterName(), 1, false)
            .editSpec()
                .withTracing(otelTracing)
                .withNewTemplate()
                        .withNewConnectContainer()
                        .addNewEnv()
                            .withName(TracingConstants.OTEL_SERVICE_ENV)
                            .withValue(JAEGER_MIRROR_MAKER2_SERVICE)
                        .endEnv()
                        .addNewEnv()
                            .withName("OTEL_EXPORTER_OTLP_ENDPOINT")
                            .withValue(JAEGER_COLLECTOR_OTLP_URL)
                        .endEnv()
                    .endConnectContainer()
                .endTemplate()
            .endSpec()
            .build());

        TracingUtils.verify(testStorage.getNamespaceName(),
            JAEGER_PRODUCER_SERVICE,
            testStorage.getScraperPodName(),
            "To_" + testStorage.getTopicName(),
            JAEGER_QUERY_SERVICE);
        TracingUtils.verify(testStorage.getNamespaceName(),
            JAEGER_CONSUMER_SERVICE, testStorage.getScraperPodName(), "From_" + testStorage.getMirroredSourceTopicName(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(testStorage.getNamespaceName(),
            JAEGER_MIRROR_MAKER2_SERVICE,
            testStorage.getScraperPodName(),
            "From_" + testStorage.getTopicName(),
            JAEGER_QUERY_SERVICE);
        TracingUtils.verify(testStorage.getNamespaceName(),
            JAEGER_MIRROR_MAKER2_SERVICE, testStorage.getScraperPodName(),
            "To_" + testStorage.getMirroredSourceTopicName(), JAEGER_QUERY_SERVICE);
    }

    @ParallelNamespaceTest
    @Tag(MIRROR_MAKER)
    void testProducerConsumerMirrorMakerService() {
        // Current implementation of Jaeger deployment and test parallelism does not allow to run this test with STRIMZI_RBAC_SCOPE=NAMESPACE`
        assumeFalse(Environment.isNamespaceRbacScope());

        // This testStorage gets resources set by BeforeEach, so it must be retrieved from storageMap
        final TestStorage testStorage = storageMap.get(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getSourceBrokerPoolName(), testStorage.getSourceClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getSourceControllerPoolName(), testStorage.getSourceClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getSourceClusterName(), 1).build());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getTargetBrokerPoolName(), testStorage.getTargetClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getTargetControllerPoolName(), testStorage.getTargetClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getTargetClusterName(), 1).build());

        // Create topic and deploy clients before Mirror Maker to not wait for MM to find the new topics
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getSourceClusterName(), testStorage.getTopicName(), 12, 3, testStorage.getNamespaceName()).build());
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getTargetClusterName(), testStorage.getTargetTopicName(), 12, 3, testStorage.getTopicName()).build());

        LOGGER.info("Setting for Kafka source plain bootstrap: {}", KafkaResources.plainBootstrapAddress(testStorage.getSourceClusterName()));

        final KafkaTracingClients sourceKafkaTracingClient =
            new KafkaTracingClientsBuilder(testStorage.getTracingClients())
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getSourceClusterName()))
                .build();

        resourceManager.createResourceWithWait(sourceKafkaTracingClient.producerWithTracing());

        LOGGER.info("Setting for Kafka target plain bootstrap: {}", KafkaResources.plainBootstrapAddress(testStorage.getTargetClusterName()));

        KafkaTracingClients targetKafkaTracingClient =  new KafkaTracingClientsBuilder(testStorage.getTracingClients())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getTargetClusterName()))
            .build();

        resourceManager.createResourceWithWait(targetKafkaTracingClient.consumerWithTracing());

        resourceManager.createResourceWithWait(KafkaMirrorMakerTemplates.kafkaMirrorMaker(testStorage.getClusterName(), testStorage.getSourceClusterName(), testStorage.getTargetClusterName(),
                ClientUtils.generateRandomConsumerGroup(), 1, false)
            .editSpec()
                .withTracing(otelTracing)
                .withNewTemplate()
                    .withNewMirrorMakerContainer()
                        .addNewEnv()
                            .withName(TracingConstants.OTEL_SERVICE_ENV)
                            .withValue(JAEGER_MIRROR_MAKER_SERVICE)
                        .endEnv()
                        .addNewEnv()
                            .withName("OTEL_EXPORTER_OTLP_ENDPOINT")
                            .withValue(JAEGER_COLLECTOR_OTLP_URL)
                        .endEnv()
                    .endMirrorMakerContainer()
                .endTemplate()
            .endSpec()
            .build());

        TracingUtils.verify(testStorage.getNamespaceName(), JAEGER_PRODUCER_SERVICE, testStorage.getScraperPodName(), "To_" + testStorage.getTopicName(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(testStorage.getNamespaceName(), JAEGER_CONSUMER_SERVICE, testStorage.getScraperPodName(), "From_" + testStorage.getTopicName(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(testStorage.getNamespaceName(), JAEGER_MIRROR_MAKER_SERVICE, testStorage.getScraperPodName(), "From_" + testStorage.getTopicName(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(testStorage.getNamespaceName(), JAEGER_MIRROR_MAKER_SERVICE, testStorage.getScraperPodName(), "To_" + testStorage.getTopicName(), JAEGER_QUERY_SERVICE);
    }

    @ParallelNamespaceTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testProducerConsumerStreamsConnectService() {
        // Current implementation of Jaeger deployment and test parallelism does not allow to run this test with STRIMZI_RBAC_SCOPE=NAMESPACE`
        assumeFalse(Environment.isNamespaceRbacScope());

        // This testStorage gets resources set by BeforeEach, so it must be retrieved from storageMap
        final TestStorage testStorage = storageMap.get(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3, 1).build());

        // Create topic and deploy clients before MirrorMaker to not wait for MM to find the new topics
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), 12, 3, testStorage.getNamespaceName()).build());
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getStreamsTopicTargetName(), 12, 3, testStorage.getNamespaceName()).build());

        final Map<String, Object> configOfKafkaConnect = new HashMap<>();
        configOfKafkaConnect.put("config.storage.replication.factor", "-1");
        configOfKafkaConnect.put("offset.storage.replication.factor", "-1");
        configOfKafkaConnect.put("status.storage.replication.factor", "-1");
        configOfKafkaConnect.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("key.converter.schemas.enable", "false");
        configOfKafkaConnect.put("value.converter.schemas.enable", "false");

        KafkaConnectBuilder connectBuilder = KafkaConnectTemplates.kafkaConnect(testStorage.getClusterName(), Environment.TEST_SUITE_NAMESPACE, 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .withNewSpec()
                .withConfig(configOfKafkaConnect)
                .withTracing(otelTracing)
                .withBootstrapServers(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
                .withReplicas(1)
                .withNewTemplate()
                    .withNewConnectContainer()
                        .addNewEnv()
                            .withName(TracingConstants.OTEL_SERVICE_ENV)
                            .withValue(JAEGER_KAFKA_CONNECT_SERVICE)
                        .endEnv()
                        .addNewEnv()
                            .withName("OTEL_EXPORTER_OTLP_ENDPOINT")
                            .withValue(JAEGER_COLLECTOR_OTLP_URL)
                        .endEnv()
                    .endConnectContainer()
                .endTemplate()
            .endSpec();

        resourceManager.createResourceWithWait(KafkaConnectTemplates.addFileSinkPluginOrImage(testStorage.getNamespaceName(), connectBuilder).build());

        resourceManager.createResourceWithWait(KafkaConnectorTemplates.kafkaConnector(testStorage.getClusterName())
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("file", TestConstants.DEFAULT_SINK_FILE_PATH)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("topics", testStorage.getTopicName())
            .endSpec()
            .build());

        resourceManager.createResourceWithWait(((KafkaTracingClients) ResourceManager.getTestContext().getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_TRACING_CLIENT_KEY)).producerWithTracing());
        resourceManager.createResourceWithWait(((KafkaTracingClients) ResourceManager.getTestContext().getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_TRACING_CLIENT_KEY)).consumerWithTracing());
        resourceManager.createResourceWithWait(((KafkaTracingClients) ResourceManager.getTestContext().getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_TRACING_CLIENT_KEY)).kafkaStreamsWithTracing());

        ClientUtils.waitForClientsSuccess(
            testStorage.getProducerName(),
            testStorage.getConsumerName(),
            testStorage.getNamespaceName(),
            testStorage.getMessageCount());

        TracingUtils.verify(testStorage.getNamespaceName(), JAEGER_PRODUCER_SERVICE, testStorage.getScraperPodName(), "To_" + testStorage.getTopicName(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(testStorage.getNamespaceName(), JAEGER_CONSUMER_SERVICE, testStorage.getScraperPodName(), "From_" + testStorage.getTopicName(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(testStorage.getNamespaceName(), JAEGER_KAFKA_CONNECT_SERVICE, testStorage.getScraperPodName(), "From_" + testStorage.getTopicName(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(testStorage.getNamespaceName(), JAEGER_KAFKA_STREAMS_SERVICE, testStorage.getScraperPodName(), "From_" + testStorage.getTopicName(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(testStorage.getNamespaceName(), JAEGER_KAFKA_STREAMS_SERVICE, testStorage.getScraperPodName(), "To_" + testStorage.getStreamsTopicTargetName(), JAEGER_QUERY_SERVICE);
    }

    @Tag(BRIDGE)
    @ParallelNamespaceTest
    void testKafkaBridgeService() {
        // This testStorage gets resources set by BeforeEach, so it must be retrieved from storageMap
        final TestStorage testStorage = storageMap.get(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3, 1)
            .editMetadata()
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .build());
        // Deploy http bridge
        resourceManager.createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(testStorage.getClusterName(), KafkaResources.plainBootstrapAddress(testStorage.getClusterName()), 1)
            .editMetadata()
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .editSpec()
                .withTracing(otelTracing)
                .withNewTemplate()
                    .withNewBridgeContainer()
                        .addNewEnv()
                            .withName(TracingConstants.OTEL_SERVICE_ENV)
                            .withValue(JAEGER_KAFKA_BRIDGE_SERVICE)
                        .endEnv()
                        .addNewEnv()
                            .withName("OTEL_EXPORTER_OTLP_ENDPOINT")
                            .withValue(JAEGER_COLLECTOR_OTLP_URL)
                        .endEnv()
                        .addNewEnv()
                            .withName("OTEL_TRACES_EXPORTER")
                            .withValue("otlp")
                        .endEnv()
                    .endBridgeContainer()
                .endTemplate()
            .endSpec()
            .build());

        final String bridgeProducer = "bridge-producer";
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), testStorage.getNamespaceName()).build());

        final BridgeTracingClients kafkaBridgeClientJob = new BridgeTracingClientsBuilder()
            .withTracingServiceNameEnvVar(TracingConstants.OTEL_SERVICE_ENV)
            .withProducerName(bridgeProducer)
            .withNamespaceName(testStorage.getNamespaceName())
            .withBootstrapAddress(KafkaBridgeResources.serviceName(testStorage.getClusterName()))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withPort(TestConstants.HTTP_BRIDGE_DEFAULT_PORT)
            .withDelayMs(1000)
            .withPollInterval(1000)
            .withOpenTelemetry()
            .build();

        resourceManager.createResourceWithWait(kafkaBridgeClientJob.producerStrimziBridgeWithTracing());
        resourceManager.createResourceWithWait((testStorage.getTracingClients()).consumerWithTracing());
        ClientUtils.waitForClientSuccess(bridgeProducer, testStorage.getNamespaceName(), testStorage.getMessageCount());

        TracingUtils.verify(testStorage.getNamespaceName(), JAEGER_KAFKA_BRIDGE_SERVICE, testStorage.getScraperPodName(), JAEGER_QUERY_SERVICE);
    }

    @Tag(BRIDGE)
    @ParallelNamespaceTest
    void testKafkaBridgeServiceWithHttpTracing() {
        // This testStorage gets resources set by BeforeEach, so it must be retrieved from storageMap
        final TestStorage testStorage = storageMap.get(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3, 1)
            .editMetadata()
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .build());
        // Deploy http bridge
        resourceManager.createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(testStorage.getClusterName(), KafkaResources.plainBootstrapAddress(testStorage.getClusterName()), 1)
            .editMetadata()
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .editSpec()
                .withTracing(otelTracing)
                .withNewTemplate()
                    .withNewBridgeContainer()
                        .addNewEnv()
                            .withName(TracingConstants.OTEL_SERVICE_ENV)
                            .withValue(JAEGER_KAFKA_BRIDGE_SERVICE)
                        .endEnv()
                        .addNewEnv()
                            .withName("OTEL_EXPORTER_OTLP_ENDPOINT")
                            .withValue(JAEGER_COLLECTOR_OTLP_URL)
                        .endEnv()
                        .addNewEnv()
                            .withName("OTEL_TRACES_EXPORTER")
                            .withValue("otlp")
                        .endEnv()
                    .endBridgeContainer()
                .endTemplate()
            .endSpec()
            .build());

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), testStorage.getNamespaceName()).build());

        final String bridgeProducer = "bridge-producer";
        final BridgeTracingClients kafkaBridgeClientJob = new BridgeTracingClientsBuilder()
            .withTracingServiceNameEnvVar(TracingConstants.OTEL_SERVICE_ENV)
            .withProducerName(bridgeProducer)
            .withNamespaceName(testStorage.getNamespaceName())
            .withBootstrapAddress(KafkaBridgeResources.serviceName(testStorage.getClusterName()))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withPort(TestConstants.HTTP_BRIDGE_DEFAULT_PORT)
            .withDelayMs(1000)
            .withPollInterval(1000)
            .withOpenTelemetry()
            .build();

        resourceManager.createResourceWithWait(kafkaBridgeClientJob.producerStrimziBridgeWithTracing());
        resourceManager.createResourceWithWait((testStorage.getTracingClients()).consumerWithTracing());
        ClientUtils.waitForClientSuccess(bridgeProducer, testStorage.getNamespaceName(), testStorage.getMessageCount());

        TracingUtils.verify(testStorage.getNamespaceName(), JAEGER_KAFKA_BRIDGE_SERVICE, testStorage.getScraperPodName(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(testStorage.getNamespaceName(), bridgeProducer, testStorage.getScraperPodName(), JAEGER_QUERY_SERVICE);
    }

    @BeforeEach
    void createTestResources() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        SetupJaeger.deployJaegerInstance(testStorage.getNamespaceName());

        resourceManager.createResourceWithWait(ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build());
        testStorage.addToTestStorage(TestConstants.SCRAPER_POD_KEY, kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName());

        final KafkaTracingClients kafkaTracingClients = new KafkaTracingClientsBuilder()
            .withNamespaceName(testStorage.getNamespaceName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withTopicName(testStorage.getTopicName())
            .withStreamsTopicTargetName(testStorage.getStreamsTopicTargetName())
            .withMessageCount(testStorage.getMessageCount())
            .withJaegerServiceProducerName(JAEGER_PRODUCER_SERVICE)
            .withJaegerServiceConsumerName(JAEGER_CONSUMER_SERVICE)
            .withJaegerServiceStreamsName(JAEGER_KAFKA_STREAMS_SERVICE)
            .withTracingServiceNameEnvVar(TracingConstants.OTEL_SERVICE_ENV)
            .withOpenTelemetry()
            .build();

        LOGGER.info("{}:\n", kafkaTracingClients.toString());

        testStorage.addToTestStorage(TestConstants.KAFKA_TRACING_CLIENT_KEY, kafkaTracingClients);

        storageMap.put(ResourceManager.getTestContext(), testStorage);
    }

    @BeforeAll
    void setup() {
        this.clusterOperator = this.clusterOperator
            .defaultInstallation()
            .createInstallation()
            .runInstallation();

        ResourceManager.STORED_RESOURCES.computeIfAbsent(ResourceManager.getTestContext().getDisplayName(), k -> new Stack<>());
        SetupJaeger.deployJaegerOperatorAndCertManager();
    }
}