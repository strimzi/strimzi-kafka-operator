/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.tracing;

import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeResources;
import io.strimzi.api.kafka.model.connect.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.resources.jaeger.SetupOpenTelemetry;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.NetworkPolicyUtils;
import io.strimzi.systemtest.utils.specific.TracingUtils;
import io.strimzi.testclients.clients.http.HttpProducerClient;
import io.strimzi.testclients.clients.http.HttpProducerClientBuilder;
import io.strimzi.testclients.clients.kafka.KafkaConsumerClient;
import io.strimzi.testclients.clients.kafka.KafkaConsumerClientBuilder;
import io.strimzi.testclients.clients.kafka.KafkaProducerClient;
import io.strimzi.testclients.clients.kafka.KafkaProducerClientBuilder;
import io.strimzi.testclients.clients.kafka.KafkaStreamsClient;
import io.strimzi.testclients.clients.kafka.KafkaStreamsClientBuilder;
import io.strimzi.testclients.configuration.Tracing;
import io.strimzi.testclients.configuration.TracingBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.HashMap;
import java.util.Map;

import static io.strimzi.systemtest.TestTags.BRIDGE;
import static io.strimzi.systemtest.TestTags.CONNECT;
import static io.strimzi.systemtest.TestTags.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.TestTags.MIRROR_MAKER2;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.TestTags.TRACING;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_COLLECTOR_OTLP_URL;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_CONSUMER_SERVICE;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_KAFKA_BRIDGE_SERVICE;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_KAFKA_CONNECT_SERVICE;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_KAFKA_STREAMS_SERVICE;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_MIRROR_MAKER2_SERVICE;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_PRODUCER_SERVICE;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_QUERY_SERVICE;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@Tag(REGRESSION)
@Tag(TRACING)
public class OpenTelemetryST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(OpenTelemetryST.class);

    @ParallelNamespaceTest
    void testProducerConsumerStreamsService() {
        final TestStorage testStorage = deployInitialResourcesAndGetTestStorage();
        final String kafkaStreamsName = testStorage.getClusterName() + "-streams";

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build());

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getClusterName(), 12, 3).build());

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getStreamsTopicTargetName(), testStorage.getClusterName(), 12, 3).build());

        final KafkaProducerClient tracingKafkaProducer = new KafkaProducerClientBuilder()
            .withName(testStorage.getProducerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withMessageCount(testStorage.getMessageCount())
            .withTracing(tracingConfiguration(JAEGER_PRODUCER_SERVICE))
            .build();

        KubeResourceManager.get().createResourceWithWait(tracingKafkaProducer.getJob());

        TracingUtils.verify(testStorage.getNamespaceName(),
            JAEGER_PRODUCER_SERVICE,
            testStorage.getScraperPodName(),
            JAEGER_QUERY_SERVICE);

        final KafkaConsumerClient tracingKafkaConsumer = new KafkaConsumerClientBuilder()
            .withName(testStorage.getConsumerName())
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withMessageCount(testStorage.getMessageCount())
            .withTracing(tracingConfiguration(JAEGER_CONSUMER_SERVICE))
            .build();

        KubeResourceManager.get().createResourceWithWait(tracingKafkaConsumer.getJob());

        TracingUtils.verify(testStorage.getNamespaceName(),
            JAEGER_CONSUMER_SERVICE,
            testStorage.getScraperPodName(),
            JAEGER_QUERY_SERVICE);

        final KafkaStreamsClient tracingKafkaStreams = new KafkaStreamsClientBuilder()
            .withName(kafkaStreamsName)
            .withNamespaceName(testStorage.getNamespaceName())
            .withSourceTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withTracing(tracingConfiguration(JAEGER_KAFKA_STREAMS_SERVICE))
            .withTargetTopicName(testStorage.getStreamsTopicTargetName())
            .withApplicationId(kafkaStreamsName)
            .build();

        KubeResourceManager.get().createResourceWithWait(tracingKafkaStreams.getJob());

        TracingUtils.verify(testStorage.getNamespaceName(),
            JAEGER_KAFKA_STREAMS_SERVICE,
            testStorage.getScraperPodName(),
            JAEGER_QUERY_SERVICE);
    }

    @ParallelNamespaceTest
    @Tag(MIRROR_MAKER2)
    void testProducerConsumerMirrorMaker2Service() {
        final TestStorage testStorage = deployInitialResourcesAndGetTestStorage();

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getSourceBrokerPoolName(), testStorage.getSourceClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getSourceControllerPoolName(), testStorage.getSourceClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getSourceClusterName(), 1).build());

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getTargetBrokerPoolName(), testStorage.getTargetClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getTargetControllerPoolName(), testStorage.getTargetClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getTargetClusterName(), 1).build());

        // Create topic and deploy clients before Mirror Maker to not wait for MM to find the new topics
        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getSourceClusterName(), 12, 3).build());

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getMirroredSourceTopicName(), testStorage.getTargetClusterName(), 12, 3).build());

        LOGGER.info("Setting for Kafka source plain bootstrap: {}", KafkaResources.plainBootstrapAddress(testStorage.getSourceClusterName()));

        final KafkaProducerClient tracingKafkaProducer = new KafkaProducerClientBuilder()
            .withName(testStorage.getProducerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getSourceClusterName()))
            .withMessageCount(testStorage.getMessageCount())
            .withTracing(tracingConfiguration(JAEGER_PRODUCER_SERVICE))
            .build();

        KubeResourceManager.get().createResourceWithWait(tracingKafkaProducer.getJob());

        LOGGER.info("Setting for Kafka target plain bootstrap: {}", KafkaResources.plainBootstrapAddress(testStorage.getTargetClusterName()));

        final KafkaConsumerClient tracingKafkaConsumer = new KafkaConsumerClientBuilder()
            .withName(testStorage.getConsumerName())
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getMirroredSourceTopicName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getTargetClusterName()))
            .withMessageCount(testStorage.getMessageCount())
            .withTracing(tracingConfiguration(JAEGER_CONSUMER_SERVICE))
            .build();

        KubeResourceManager.get().createResourceWithWait(tracingKafkaConsumer.getJob());

        KubeResourceManager.get().createResourceWithWait(KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage.getNamespaceName(), testStorage.getClusterName(), testStorage.getSourceClusterName(), testStorage.getTargetClusterName(), 1, false)
            .editSpec()
                .withNewOpenTelemetryTracing()
                .endOpenTelemetryTracing()
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
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testProducerConsumerStreamsConnectService() {
        final TestStorage testStorage = deployInitialResourcesAndGetTestStorage();
        final String kafkaStreamsName = testStorage.getClusterName() + "-streams";

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build());

        // Create topic and deploy clients before MirrorMaker to not wait for MM to find the new topics
        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getClusterName(), 12, 3).build());
        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getStreamsTopicTargetName(), testStorage.getClusterName(), 12, 3).build());

        final Map<String, Object> configOfKafkaConnect = new HashMap<>();
        configOfKafkaConnect.put("config.storage.replication.factor", "-1");
        configOfKafkaConnect.put("offset.storage.replication.factor", "-1");
        configOfKafkaConnect.put("status.storage.replication.factor", "-1");
        configOfKafkaConnect.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("key.converter.schemas.enable", "false");
        configOfKafkaConnect.put("value.converter.schemas.enable", "false");

        KafkaConnectBuilder connectBuilder = KafkaConnectTemplates.kafkaConnect(testStorage.getNamespaceName(), testStorage.getClusterName(), 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .withNewSpec()
                .withGroupId(KafkaConnectResources.componentName(testStorage.getClusterName()))
                .withConfigStorageTopic(KafkaConnectResources.configMapName(testStorage.getClusterName()))
                .withOffsetStorageTopic(KafkaConnectResources.configStorageTopicOffsets(testStorage.getClusterName()))
                .withStatusStorageTopic(KafkaConnectResources.configStorageTopicStatus(testStorage.getClusterName()))
                .withConfig(configOfKafkaConnect)
                .withNewOpenTelemetryTracing()
                .endOpenTelemetryTracing()
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

        KubeResourceManager.get().createResourceWithWait(KafkaConnectTemplates.addFileSinkPluginOrImage(testStorage.getNamespaceName(), connectBuilder).build());

        KubeResourceManager.get().createResourceWithWait(KafkaConnectorTemplates.kafkaConnector(testStorage.getNamespaceName(), testStorage.getClusterName())
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("file", TestConstants.DEFAULT_SINK_FILE_PATH)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("topics", testStorage.getTopicName())
            .endSpec()
            .build());

        final KafkaProducerClient tracingKafkaProducer = new KafkaProducerClientBuilder()
            .withName(testStorage.getProducerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withMessageCount(testStorage.getMessageCount())
            .withTracing(tracingConfiguration(JAEGER_PRODUCER_SERVICE))
            .build();

        KubeResourceManager.get().createResourceWithWait(tracingKafkaProducer.getJob());

        final KafkaConsumerClient tracingKafkaConsumer = new KafkaConsumerClientBuilder()
            .withName(testStorage.getConsumerName())
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withMessageCount(testStorage.getMessageCount())
            .withTracing(tracingConfiguration(JAEGER_CONSUMER_SERVICE))
            .build();

        KubeResourceManager.get().createResourceWithWait(tracingKafkaConsumer.getJob());

        final KafkaStreamsClient tracingKafkaStreams = new KafkaStreamsClientBuilder()
            .withName(kafkaStreamsName)
            .withNamespaceName(testStorage.getNamespaceName())
            .withSourceTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withTracing(tracingConfiguration(JAEGER_KAFKA_STREAMS_SERVICE))
            .withTargetTopicName(testStorage.getStreamsTopicTargetName())
            .withApplicationId(kafkaStreamsName)
            .build();

        KubeResourceManager.get().createResourceWithWait(tracingKafkaStreams.getJob());

        ClientUtils.waitForClientsSuccess(
            testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getProducerName(),
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
        final TestStorage testStorage = deployInitialResourcesAndGetTestStorage();

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build());
        // Deploy http bridge
        KubeResourceManager.get().createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaResources.plainBootstrapAddress(testStorage.getClusterName()), 1)
            .editSpec()
                .withNewOpenTelemetryTracing()
                .endOpenTelemetryTracing()
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
        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getClusterName()).build());

        // Create NetworkPolicy for HTTP producer to access Bridge
        NetworkPolicyUtils.allowNetworkPolicyForBridgeClient(testStorage.getNamespaceName(), testStorage.getClusterName(), bridgeProducer);

        HttpProducerClient httpProducer = new HttpProducerClientBuilder()
            .withNamespaceName(testStorage.getNamespaceName())
            .withName(bridgeProducer)
            .withHostname(KafkaBridgeResources.serviceName(testStorage.getClusterName()))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withPort(TestConstants.HTTP_BRIDGE_DEFAULT_PORT)
            .withDelayMs(1000)
            .withTracing(tracingConfiguration(bridgeProducer))
            .addToAdditionalEnvVars()
            .build();

        KubeResourceManager.get().createResourceWithWait(httpProducer.getJob());

        final KafkaConsumerClient tracingKafkaConsumer = new KafkaConsumerClientBuilder()
            .withName(testStorage.getConsumerName())
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withMessageCount(testStorage.getMessageCount())
            .withTracing(tracingConfiguration(JAEGER_CONSUMER_SERVICE))
            .build();

        KubeResourceManager.get().createResourceWithWait(tracingKafkaConsumer.getJob());

        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), bridgeProducer, testStorage.getMessageCount());

        TracingUtils.verify(testStorage.getNamespaceName(), JAEGER_KAFKA_BRIDGE_SERVICE, testStorage.getScraperPodName(), JAEGER_QUERY_SERVICE);
    }

    @Tag(BRIDGE)
    @ParallelNamespaceTest
    void testKafkaBridgeServiceWithHttpTracing() {
        final TestStorage testStorage = deployInitialResourcesAndGetTestStorage();

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build());
        // Deploy http bridge
        KubeResourceManager.get().createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaResources.plainBootstrapAddress(testStorage.getClusterName()), 1)
            .editSpec()
                .withNewOpenTelemetryTracing()
                .endOpenTelemetryTracing()
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

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getClusterName()).build());

        final String bridgeProducer = "bridge-producer";

        // Create NetworkPolicy for HTTP producer to access Bridge
        NetworkPolicyUtils.allowNetworkPolicyForBridgeClient(testStorage.getNamespaceName(), testStorage.getClusterName(), bridgeProducer);

        HttpProducerClient httpProducer = new HttpProducerClientBuilder()
            .withNamespaceName(testStorage.getNamespaceName())
            .withName(bridgeProducer)
            .withHostname(KafkaBridgeResources.serviceName(testStorage.getClusterName()))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withPort(TestConstants.HTTP_BRIDGE_DEFAULT_PORT)
            .withDelayMs(1000)
            .withTracing(tracingConfiguration(bridgeProducer))
            .addToAdditionalEnvVars()
            .build();

        KubeResourceManager.get().createResourceWithWait(httpProducer.getJob());

        final KafkaConsumerClient tracingKafkaConsumer = new KafkaConsumerClientBuilder()
            .withName(testStorage.getConsumerName())
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withMessageCount(testStorage.getMessageCount())
            .withTracing(tracingConfiguration(JAEGER_CONSUMER_SERVICE))
            .build();

        KubeResourceManager.get().createResourceWithWait(tracingKafkaConsumer.getJob());

        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), bridgeProducer, testStorage.getMessageCount());

        TracingUtils.verify(testStorage.getNamespaceName(), JAEGER_KAFKA_BRIDGE_SERVICE, testStorage.getScraperPodName(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(testStorage.getNamespaceName(), bridgeProducer, testStorage.getScraperPodName(), JAEGER_QUERY_SERVICE);
    }

    private TestStorage deployInitialResourcesAndGetTestStorage() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        SetupOpenTelemetry.deployJaegerInstance(testStorage.getNamespaceName());

        KubeResourceManager.get().createResourceWithWait(ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build());
        testStorage.addToTestStorage(TestConstants.SCRAPER_POD_KEY, KubeResourceManager.get().kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName());

        return testStorage;
    }

    private Tracing tracingConfiguration(String serviceName) {
        return new TracingBuilder()
            .withServiceNameEnvVar(TracingConstants.OTEL_SERVICE_ENV)
            .withServiceName(serviceName)
            .withTracingType(TracingConstants.OPEN_TELEMETRY)
            .withAdditionalTracingEnvVars(new EnvVarBuilder()
                .withName("OTEL_EXPORTER_OTLP_ENDPOINT")
                .withValue(JAEGER_COLLECTOR_OTLP_URL)
                .build()
            )
            .build();
    }

    @BeforeAll
    void setup() {
        assumeFalse(Environment.isNamespaceRbacScope());

        SetupClusterOperator
            .getInstance()
            .withDefaultConfiguration()
            .install();

        SetupOpenTelemetry.deployOpenTelemetryOperatorAndCertManager();
    }
}
