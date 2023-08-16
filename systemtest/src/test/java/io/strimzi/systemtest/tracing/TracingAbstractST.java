/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.tracing;

import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyBuilder;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.connect.build.JarArtifactBuilder;
import io.strimzi.api.kafka.model.connect.build.PluginBuilder;
import io.strimzi.api.kafka.model.tracing.Tracing;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeTracingClients;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeTracingClientsBuilder;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaTracingClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaTracingClientsBuilder;
import io.strimzi.systemtest.resources.ResourceItem;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMakerTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.specific.TracingUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.logs.CollectorElement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Stack;

import static io.strimzi.systemtest.Constants.JAEGER_DEPLOYMENT_POLL;
import static io.strimzi.systemtest.Constants.JAEGER_DEPLOYMENT_TIMEOUT;
import static io.strimzi.systemtest.Constants.KAFKA_TRACING_CLIENT_KEY;
import static io.strimzi.systemtest.tracing.TracingConstants.CERT_MANAGER_DEPLOYMENT;
import static io.strimzi.systemtest.tracing.TracingConstants.CERT_MANAGER_CA_INJECTOR_DEPLOYMENT;
import static io.strimzi.systemtest.tracing.TracingConstants.CERT_MANAGER_NAMESPACE;
import static io.strimzi.systemtest.tracing.TracingConstants.CERT_MANAGER_WEBHOOK_DEPLOYMENT;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_COLLECTOR_OTLP_URL;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_CONSUMER_SERVICE;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_INSTANCE_NAME;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_KAFKA_BRIDGE_SERVICE;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_KAFKA_CONNECT_SERVICE;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_KAFKA_STREAMS_SERVICE;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_MIRROR_MAKER2_SERVICE;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_MIRROR_MAKER_SERVICE;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_OPERATOR_DEPLOYMENT_NAME;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_PRODUCER_SERVICE;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_QUERY_SERVICE;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Provides a general test cases (e.g., test that we are able to produce and consume messages, and then we could see traces
 * in jaeger API) for the inherited classes (i.e., {@link OpenTelemetryST}).
 */
public abstract class TracingAbstractST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(TracingAbstractST.class);

    private String jaegerConfigs;

    private final String certManagerPath = TestUtils.USER_PATH + "/../systemtest/src/test/resources/tracing/cert-manager.yaml";
    private final String jaegerInstancePath = TestUtils.USER_PATH + "/../systemtest/src/test/resources/tracing/jaeger-instance.yaml";
    private final String jaegerOperatorPath = TestUtils.USER_PATH + "/../systemtest/src/test/resources/tracing/jaeger-operator.yaml";

    protected abstract Tracing tracing();
    protected abstract String serviceNameEnvVar();

    void doTestProducerConsumerStreamsService(final ExtensionContext extensionContext) {
        // Current implementation of Jaeger deployment and test parallelism does not allow to run this test with STRIMZI_RBAC_SCOPE=NAMESPACE`
        assumeFalse(Environment.isNamespaceRbacScope());

        resourceManager.createResourceWithWait(extensionContext,
            KafkaTemplates.kafkaEphemeral(
                storageMap.get(extensionContext).getClusterName(), 3, 1).build());

        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(storageMap.get(extensionContext).getClusterName(), storageMap.get(extensionContext).getTopicName(), 12, 3, storageMap.get(extensionContext).getNamespaceName()).build());

        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(storageMap.get(extensionContext).getClusterName(), storageMap.get(extensionContext).retrieveFromTestStorage(Constants.STREAM_TOPIC_KEY).toString(), 12, 3, storageMap.get(extensionContext).getNamespaceName()).build());

        resourceManager.createResourceWithWait(extensionContext, ((KafkaTracingClients) storageMap.get(extensionContext).retrieveFromTestStorage(KAFKA_TRACING_CLIENT_KEY)).producerWithTracing());

        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(),
            JAEGER_PRODUCER_SERVICE,
            storageMap.get(extensionContext).retrieveFromTestStorage(Constants.SCRAPER_POD_KEY).toString(),
            JAEGER_QUERY_SERVICE);

        resourceManager.createResourceWithWait(extensionContext, ((KafkaTracingClients) storageMap.get(extensionContext).retrieveFromTestStorage(KAFKA_TRACING_CLIENT_KEY)).consumerWithTracing());

        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(),
            JAEGER_CONSUMER_SERVICE,
            storageMap.get(extensionContext).retrieveFromTestStorage(Constants.SCRAPER_POD_KEY).toString(),
            JAEGER_QUERY_SERVICE);

        resourceManager.createResourceWithWait(extensionContext, ((KafkaTracingClients) storageMap.get(extensionContext).retrieveFromTestStorage(KAFKA_TRACING_CLIENT_KEY)).kafkaStreamsWithTracing());

        // Disabled for OpenTracing, because of issue with Streams API and tracing https://github.com/strimzi/strimzi-kafka-operator/issues/5680
        if (this.getClass().getSimpleName().contains(TracingConstants.OPEN_TELEMETRY)) {
            TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(),
                JAEGER_KAFKA_STREAMS_SERVICE,
                storageMap.get(extensionContext).retrieveFromTestStorage(Constants.SCRAPER_POD_KEY).toString(),
                JAEGER_QUERY_SERVICE);
        }
    }

    void doTestProducerConsumerMirrorMaker2Service(final ExtensionContext extensionContext) {
        // Current implementation of Jaeger deployment and test parallelism does not allow to run this test with STRIMZI_RBAC_SCOPE=NAMESPACE`
        assumeFalse(Environment.isNamespaceRbacScope());

        final String kafkaClusterSourceName = storageMap.get(extensionContext).getClusterName();
        final String kafkaClusterTargetName = storageMap.get(extensionContext).getClusterName() + "-target";

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 3, 1).build());
        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 3, 1).build());

        // Create topic and deploy clients before Mirror Maker to not wait for MM to find the new topics
        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, storageMap.get(extensionContext).getTopicName(), 12, 3, storageMap.get(extensionContext).getNamespaceName()).build());

        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(kafkaClusterTargetName, kafkaClusterSourceName + "." + storageMap.get(extensionContext).getTopicName(), 12, 3, storageMap.get(extensionContext).getNamespaceName()).build());

        LOGGER.info("Setting for Kafka source plain bootstrap: {}", KafkaResources.plainBootstrapAddress(kafkaClusterSourceName));

        KafkaTracingClients sourceKafkaTracingClient = new KafkaTracingClientsBuilder((KafkaTracingClients) storageMap.get(extensionContext).retrieveFromTestStorage(KAFKA_TRACING_CLIENT_KEY))
            .withTopicName(storageMap.get(extensionContext).getTopicName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName))
            .build();

        resourceManager.createResourceWithWait(extensionContext, sourceKafkaTracingClient.producerWithTracing());

        LOGGER.info("Setting for Kafka target plain bootstrap: {}", KafkaResources.plainBootstrapAddress(kafkaClusterTargetName));

        final KafkaTracingClients targetKafkaTracingClient = new KafkaTracingClientsBuilder((KafkaTracingClients) storageMap.get(extensionContext).retrieveFromTestStorage(KAFKA_TRACING_CLIENT_KEY))
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))
            .withTopicName(kafkaClusterSourceName + "." + storageMap.get(extensionContext).getTopicName())
            .build();

        resourceManager.createResourceWithWait(extensionContext, targetKafkaTracingClient.consumerWithTracing());

        resourceManager.createResourceWithWait(extensionContext, KafkaMirrorMaker2Templates.kafkaMirrorMaker2(storageMap.get(extensionContext).getClusterName(), kafkaClusterTargetName, kafkaClusterSourceName, 1, false)
            .editSpec()
            .withTracing(tracing())
                .withNewTemplate()
                    .withNewConnectContainer()
                        .addNewEnv()
                            .withName(this.serviceNameEnvVar())
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

        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(),
            JAEGER_PRODUCER_SERVICE,
            storageMap.get(extensionContext).retrieveFromTestStorage(Constants.SCRAPER_POD_KEY).toString(),
            "To_" + storageMap.get(extensionContext).getTopicName(),
            JAEGER_QUERY_SERVICE);
        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(),
            JAEGER_CONSUMER_SERVICE, storageMap.get(extensionContext).retrieveFromTestStorage(Constants.SCRAPER_POD_KEY).toString(), "From_" + kafkaClusterSourceName + "." + storageMap.get(extensionContext).getTopicName(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(),
            JAEGER_MIRROR_MAKER2_SERVICE,
            storageMap.get(extensionContext).retrieveFromTestStorage(Constants.SCRAPER_POD_KEY).toString(),
            "From_" + storageMap.get(extensionContext).getTopicName(),
            JAEGER_QUERY_SERVICE);
        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(),
            JAEGER_MIRROR_MAKER2_SERVICE, storageMap.get(extensionContext).retrieveFromTestStorage(Constants.SCRAPER_POD_KEY).toString(),
            "To_" + kafkaClusterSourceName + "." + storageMap.get(extensionContext).getTopicName(), JAEGER_QUERY_SERVICE);
    }

    void doTestProducerConsumerMirrorMakerService(final ExtensionContext extensionContext) {
        // Current implementation of Jaeger deployment and test parallelism does not allow to run this test with STRIMZI_RBAC_SCOPE=NAMESPACE`
        assumeFalse(Environment.isNamespaceRbacScope());

        final String kafkaClusterSourceName = storageMap.get(extensionContext).getClusterName();
        final String kafkaClusterTargetName = storageMap.get(extensionContext).getClusterName() + "-target";

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 3, 1).build());
        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 3, 1).build());

        // Create topic and deploy clients before Mirror Maker to not wait for MM to find the new topics
        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, storageMap.get(extensionContext).getTopicName(), 12, 3, storageMap.get(extensionContext).getNamespaceName()).build());


        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(kafkaClusterTargetName, storageMap.get(extensionContext).getTopicName() + "-target", 12, 3, storageMap.get(extensionContext).getTopicName()).build());

        LOGGER.info("Setting for Kafka source plain bootstrap: {}", KafkaResources.plainBootstrapAddress(kafkaClusterSourceName));

        final KafkaTracingClients sourceKafkaTracingClient =
            new KafkaTracingClientsBuilder((KafkaTracingClients) storageMap.get(extensionContext).retrieveFromTestStorage(KAFKA_TRACING_CLIENT_KEY))
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName))
                .build();

        resourceManager.createResourceWithWait(extensionContext, sourceKafkaTracingClient.producerWithTracing());

        LOGGER.info("Setting for Kafka target plain bootstrap: {}", KafkaResources.plainBootstrapAddress(kafkaClusterTargetName));

        KafkaTracingClients targetKafkaTracingClient =  new KafkaTracingClientsBuilder((KafkaTracingClients) storageMap.get(extensionContext).retrieveFromTestStorage(KAFKA_TRACING_CLIENT_KEY))
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))
            .build();

        resourceManager.createResourceWithWait(extensionContext, targetKafkaTracingClient.consumerWithTracing());

        resourceManager.createResourceWithWait(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(storageMap.get(extensionContext).getClusterName(), kafkaClusterSourceName, kafkaClusterTargetName,
                ClientUtils.generateRandomConsumerGroup(), 1, false)
            .editSpec()
            .withTracing(tracing())
                .withNewTemplate()
                    .withNewMirrorMakerContainer()
                        .addNewEnv()
                            .withName(this.serviceNameEnvVar())
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

        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(), JAEGER_PRODUCER_SERVICE, storageMap.get(extensionContext).retrieveFromTestStorage(Constants.SCRAPER_POD_KEY).toString(), "To_" + storageMap.get(extensionContext).getTopicName(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(), JAEGER_CONSUMER_SERVICE, storageMap.get(extensionContext).retrieveFromTestStorage(Constants.SCRAPER_POD_KEY).toString(), "From_" + storageMap.get(extensionContext).getTopicName(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(), JAEGER_MIRROR_MAKER_SERVICE, storageMap.get(extensionContext).retrieveFromTestStorage(Constants.SCRAPER_POD_KEY).toString(), "From_" + storageMap.get(extensionContext).getTopicName(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(), JAEGER_MIRROR_MAKER_SERVICE, storageMap.get(extensionContext).retrieveFromTestStorage(Constants.SCRAPER_POD_KEY).toString(), "To_" + storageMap.get(extensionContext).getTopicName(), JAEGER_QUERY_SERVICE);
    }

    void doTestProducerConsumerStreamsConnectService(final ExtensionContext extensionContext) {
        // Current implementation of Jaeger deployment and test parallelism does not allow to run this test with STRIMZI_RBAC_SCOPE=NAMESPACE`
        assumeFalse(Environment.isNamespaceRbacScope());

        final String imageFullPath = Environment.getImageOutputRegistry(storageMap.get(extensionContext).getNamespaceName(), Constants.ST_CONNECT_BUILD_IMAGE_NAME, String.valueOf(new Random().nextInt(Integer.MAX_VALUE)));

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(storageMap.get(extensionContext).getClusterName(), 3, 1).build());

        // Create topic and deploy clients before MirrorMaker to not wait for MM to find the new topics
        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(storageMap.get(extensionContext).getClusterName(), storageMap.get(extensionContext).getTopicName(), 12, 3, storageMap.get(extensionContext).getNamespaceName()).build());


        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(storageMap.get(extensionContext).getClusterName(), storageMap.get(extensionContext).retrieveFromTestStorage(Constants.STREAM_TOPIC_KEY).toString(), 12, 3, storageMap.get(extensionContext).getNamespaceName()).build());

        final Map<String, Object> configOfKafkaConnect = new HashMap<>();
        configOfKafkaConnect.put("config.storage.replication.factor", "-1");
        configOfKafkaConnect.put("offset.storage.replication.factor", "-1");
        configOfKafkaConnect.put("status.storage.replication.factor", "-1");
        configOfKafkaConnect.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("key.converter.schemas.enable", "false");
        configOfKafkaConnect.put("value.converter.schemas.enable", "false");

        resourceManager.createResourceWithWait(extensionContext, KafkaConnectTemplates.kafkaConnect(storageMap.get(extensionContext).getClusterName(), Constants.TEST_SUITE_NAMESPACE, 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .withNewSpec()
                .withConfig(configOfKafkaConnect)
                .withTracing(tracing())
                .withBootstrapServers(KafkaResources.plainBootstrapAddress(storageMap.get(extensionContext).getClusterName()))
                .withReplicas(1)
                .withNewTemplate()
                    .withNewConnectContainer()
                        .addNewEnv()
                            .withName(this.serviceNameEnvVar())
                            .withValue(JAEGER_KAFKA_CONNECT_SERVICE)
                        .endEnv()
                        .addNewEnv()
                            .withName("OTEL_EXPORTER_OTLP_ENDPOINT")
                            .withValue(JAEGER_COLLECTOR_OTLP_URL)
                        .endEnv()
                    .endConnectContainer()
                .endTemplate()
                // we need to set this for correct usage of the File plugin - because we need new spec, the kafkaConnectWithFilePlugin
                // method cannot be used
                .editOrNewBuild()
                    .withPlugins(new PluginBuilder()
                        .withName("file-plugin")
                        .withArtifacts(
                            new JarArtifactBuilder()
                                .withUrl(Environment.ST_FILE_PLUGIN_URL)
                                .build()
                        )
                        .build())
                    .withOutput(KafkaConnectTemplates.dockerOutput(imageFullPath))
                .endBuild()
            .endSpec()
            .build());

        resourceManager.createResourceWithWait(extensionContext, KafkaConnectorTemplates.kafkaConnector(storageMap.get(extensionContext).getClusterName())
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("file", Constants.DEFAULT_SINK_FILE_PATH)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("topics", storageMap.get(extensionContext).getTopicName())
            .endSpec()
            .build());

        resourceManager.createResourceWithWait(extensionContext, ((KafkaTracingClients) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_TRACING_CLIENT_KEY)).producerWithTracing());
        resourceManager.createResourceWithWait(extensionContext, ((KafkaTracingClients) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_TRACING_CLIENT_KEY)).consumerWithTracing());
        resourceManager.createResourceWithWait(extensionContext, ((KafkaTracingClients) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_TRACING_CLIENT_KEY)).kafkaStreamsWithTracing());

        ClientUtils.waitForClientsSuccess(
            storageMap.get(extensionContext).getProducerName(),
            storageMap.get(extensionContext).getConsumerName(),
            storageMap.get(extensionContext).getNamespaceName(), MESSAGE_COUNT);

        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(), JAEGER_PRODUCER_SERVICE, storageMap.get(extensionContext).retrieveFromTestStorage(Constants.SCRAPER_POD_KEY).toString(), "To_" + storageMap.get(extensionContext).getTopicName(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(), JAEGER_CONSUMER_SERVICE, storageMap.get(extensionContext).retrieveFromTestStorage(Constants.SCRAPER_POD_KEY).toString(), "From_" + storageMap.get(extensionContext).getTopicName(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(), JAEGER_KAFKA_CONNECT_SERVICE, storageMap.get(extensionContext).retrieveFromTestStorage(Constants.SCRAPER_POD_KEY).toString(), "From_" + storageMap.get(extensionContext).getTopicName(), JAEGER_QUERY_SERVICE);
        // Disabled for OpenTracing, because of issue with Streams API and tracing https://github.com/strimzi/strimzi-kafka-operator/issues/5680
        if (this.getClass().getSimpleName().contains(TracingConstants.OPEN_TELEMETRY)) {
            TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(), JAEGER_KAFKA_STREAMS_SERVICE, storageMap.get(extensionContext).retrieveFromTestStorage(Constants.SCRAPER_POD_KEY).toString(), "From_" + storageMap.get(extensionContext).getTopicName(), JAEGER_QUERY_SERVICE);
            TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(), JAEGER_KAFKA_STREAMS_SERVICE, storageMap.get(extensionContext).retrieveFromTestStorage(Constants.SCRAPER_POD_KEY).toString(), "To_" + storageMap.get(extensionContext).retrieveFromTestStorage(Constants.STREAM_TOPIC_KEY).toString(), JAEGER_QUERY_SERVICE);
        }
    }

    void doTestKafkaBridgeService(final ExtensionContext extensionContext) {
        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(storageMap.get(extensionContext).getClusterName(), 3, 1).build());
        // Deploy http bridge
        resourceManager.createResourceWithWait(extensionContext, KafkaBridgeTemplates.kafkaBridge(storageMap.get(extensionContext).getClusterName(), KafkaResources.plainBootstrapAddress(storageMap.get(extensionContext).getClusterName()), 1)
            .editSpec()
                .withTracing(tracing())
                .withNewTemplate()
                    .withNewBridgeContainer()
                        .addNewEnv()
                            .withName(this.serviceNameEnvVar())
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
        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(storageMap.get(extensionContext).getClusterName(), storageMap.get(extensionContext).getTopicName(), storageMap.get(extensionContext).getNamespaceName()).build());

        final BridgeTracingClientsBuilder kafkaBridgeClientJobBuilder = new BridgeTracingClientsBuilder()
            .withTracingServiceNameEnvVar(this.serviceNameEnvVar())
            .withProducerName(bridgeProducer)
            .withNamespaceName(storageMap.get(extensionContext).getNamespaceName())
            .withBootstrapAddress(KafkaBridgeResources.serviceName(storageMap.get(extensionContext).getClusterName()))
            .withTopicName(storageMap.get(extensionContext).getTopicName())
            .withMessageCount(MESSAGE_COUNT)
            .withPort(Constants.HTTP_BRIDGE_DEFAULT_PORT)
            .withDelayMs(1000)
            .withPollInterval(1000);
        final BridgeTracingClients bridgeTracingClientJob = (BridgeTracingClients) this.configureProperTracingType(kafkaBridgeClientJobBuilder.build());

        resourceManager.createResourceWithWait(extensionContext, bridgeTracingClientJob.producerStrimziBridgeWithTracing());
        resourceManager.createResourceWithWait(extensionContext, ((KafkaTracingClients) storageMap.get(extensionContext).retrieveFromTestStorage(KAFKA_TRACING_CLIENT_KEY)).consumerWithTracing());
        ClientUtils.waitForClientSuccess(bridgeProducer, storageMap.get(extensionContext).getNamespaceName(), MESSAGE_COUNT);

        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(), JAEGER_KAFKA_BRIDGE_SERVICE, storageMap.get(extensionContext).retrieveFromTestStorage(Constants.SCRAPER_POD_KEY).toString(), JAEGER_QUERY_SERVICE);
    }

    void doTestKafkaBridgeServiceWithHttpTracing(final ExtensionContext extensionContext) {
        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(storageMap.get(extensionContext).getClusterName(), 3, 1).build());
        // Deploy http bridge
        resourceManager.createResourceWithWait(extensionContext, KafkaBridgeTemplates.kafkaBridge(storageMap.get(extensionContext).getClusterName(), KafkaResources.plainBootstrapAddress(storageMap.get(extensionContext).getClusterName()), 1)
            .editSpec()
            .withTracing(tracing())
            .withNewTemplate()
                .withNewBridgeContainer()
                    .addNewEnv()
                        .withName(this.serviceNameEnvVar())
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

        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(storageMap.get(extensionContext).getClusterName(), storageMap.get(extensionContext).getTopicName(), storageMap.get(extensionContext).getNamespaceName()).build());

        final String bridgeProducer = "bridge-producer";
        final BridgeTracingClientsBuilder kafkaBridgeClientJobBuilder = new BridgeTracingClientsBuilder()
            .withTracingServiceNameEnvVar(this.serviceNameEnvVar())
            .withProducerName(bridgeProducer)
            .withNamespaceName(storageMap.get(extensionContext).getNamespaceName())
            .withBootstrapAddress(KafkaBridgeResources.serviceName(storageMap.get(extensionContext).getClusterName()))
            .withTopicName(storageMap.get(extensionContext).getTopicName())
            .withMessageCount(MESSAGE_COUNT)
            .withPort(Constants.HTTP_BRIDGE_DEFAULT_PORT)
            .withDelayMs(1000)
            .withPollInterval(1000);
        final BridgeTracingClients bridgeTracingClients = (BridgeTracingClients) this.configureProperTracingType(kafkaBridgeClientJobBuilder.build());

        resourceManager.createResourceWithWait(extensionContext, bridgeTracingClients.producerStrimziBridgeWithTracing());
        resourceManager.createResourceWithWait(extensionContext, ((KafkaTracingClients) storageMap.get(extensionContext).retrieveFromTestStorage(KAFKA_TRACING_CLIENT_KEY)).consumerWithTracing());
        ClientUtils.waitForClientSuccess(bridgeProducer, storageMap.get(extensionContext).getNamespaceName(), MESSAGE_COUNT);

        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(), JAEGER_KAFKA_BRIDGE_SERVICE, storageMap.get(extensionContext).retrieveFromTestStorage(Constants.SCRAPER_POD_KEY).toString(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(storageMap.get(extensionContext).getNamespaceName(), bridgeProducer, storageMap.get(extensionContext).retrieveFromTestStorage(Constants.SCRAPER_POD_KEY).toString(), JAEGER_QUERY_SERVICE);
    }

    /**
     * Delete Jaeger instance
     */
    private void deleteJaeger() {
        cmdKubeClient().namespace(Constants.TEST_SUITE_NAMESPACE).deleteContent(this.jaegerConfigs);
    }

    private void deleteCertManager() {
        cmdKubeClient().delete(certManagerPath);
        DeploymentUtils.waitForDeploymentDeletion(CERT_MANAGER_NAMESPACE, CERT_MANAGER_DEPLOYMENT);
        DeploymentUtils.waitForDeploymentDeletion(CERT_MANAGER_NAMESPACE, CERT_MANAGER_WEBHOOK_DEPLOYMENT);
        DeploymentUtils.waitForDeploymentDeletion(CERT_MANAGER_NAMESPACE, CERT_MANAGER_CA_INJECTOR_DEPLOYMENT);
    }

    private void deployCertManager(ExtensionContext extensionContext) {
        // create namespace `cert-manager` and add it to stack, to collect logs from it
        cluster.createNamespace(CollectorElement.createCollectorElement(extensionContext.getRequiredTestClass().getName()), CERT_MANAGER_NAMESPACE);
        StUtils.copyImagePullSecrets(CERT_MANAGER_NAMESPACE);

        LOGGER.info("Deploying CertManager from {}", certManagerPath);
        // because we don't want to apply CertManager's file to specific namespace, passing the empty String will do the trick
        cmdKubeClient("").apply(certManagerPath);

        ResourceManager.STORED_RESOURCES.get(extensionContext.getDisplayName()).push(new ResourceItem<>(this::deleteCertManager));
    }

    private void waitForCertManagerDeployment() {
        DeploymentUtils.waitForDeploymentAndPodsReady(CERT_MANAGER_NAMESPACE, CERT_MANAGER_DEPLOYMENT, 1);
        DeploymentUtils.waitForDeploymentAndPodsReady(CERT_MANAGER_NAMESPACE, CERT_MANAGER_WEBHOOK_DEPLOYMENT, 1);
        DeploymentUtils.waitForDeploymentAndPodsReady(CERT_MANAGER_NAMESPACE, CERT_MANAGER_CA_INJECTOR_DEPLOYMENT, 1);
    }

    private void deployAndWaitForCertManager(final ExtensionContext extensionContext) {
        this.deployCertManager(extensionContext);
        this.waitForCertManagerDeployment();
    }

    private void deployJaegerContent(ExtensionContext extensionContext) {
        TestUtils.waitFor("Jaeger deploy", JAEGER_DEPLOYMENT_POLL, JAEGER_DEPLOYMENT_TIMEOUT, () -> {
            try {
                String jaegerOperator = Files.readString(Paths.get(jaegerOperatorPath)).replace("observability", Constants.TEST_SUITE_NAMESPACE);

                this.jaegerConfigs = jaegerOperator;
                LOGGER.info("Creating Jaeger Operator (and needed resources) from {}", jaegerOperatorPath);
                cmdKubeClient(Constants.TEST_SUITE_NAMESPACE).applyContent(jaegerOperator);
                return true;
            } catch (Exception e) {
                LOGGER.error("{} - Exception {} has been thrown during Jaeger Deployment" + e.getMessage());
                return false;
            }
        });
        ResourceManager.STORED_RESOURCES.get(extensionContext.getDisplayName()).push(new ResourceItem<>(this::deleteJaeger));
        DeploymentUtils.waitForDeploymentAndPodsReady(Constants.TEST_SUITE_NAMESPACE, JAEGER_OPERATOR_DEPLOYMENT_NAME, 1);
    }

    private void deployJaegerOperator(final ExtensionContext extensionContext) {
        LOGGER.info("=== Applying Jaeger Operator install files ===");

        this.deployJaegerContent(extensionContext);

        NetworkPolicy networkPolicy = new NetworkPolicyBuilder()
            .withApiVersion("networking.k8s.io/v1")
            .withKind(Constants.NETWORK_POLICY)
            .withNewMetadata()
                .withName("jaeger-allow")
                .withNamespace(Constants.TEST_SUITE_NAMESPACE)
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
        resourceManager.createResourceWithWait(extensionContext, networkPolicy);
        LOGGER.info("Network policy for jaeger successfully created");
    }

    /**
     * Install of Jaeger instance
     */
    private void deployJaegerInstance(final ExtensionContext extensionContext, String namespaceName) {
        LOGGER.info("=== Applying jaeger instance install file ===");

        String instanceYamlContent = TestUtils.getContent(new File(jaegerInstancePath), TestUtils::toYamlString);
        cmdKubeClient(namespaceName).applyContent(instanceYamlContent);
        ResourceManager.STORED_RESOURCES.computeIfAbsent(extensionContext.getDisplayName(), k -> new Stack<>());
        ResourceManager.STORED_RESOURCES.get(extensionContext.getDisplayName()).push(new ResourceItem<>(() -> cmdKubeClient(namespaceName).deleteContent(instanceYamlContent)));
        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, JAEGER_INSTANCE_NAME, 1);
    }

    @BeforeEach
    void createTestResources(final ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, Constants.TEST_SUITE_NAMESPACE);

        storageMap.put(extensionContext, testStorage);

        this.deployJaegerInstance(extensionContext, storageMap.get(extensionContext).getNamespaceName());

        resourceManager.createResourceWithWait(extensionContext, ScraperTemplates.scraperPod(storageMap.get(extensionContext).getNamespaceName(), storageMap.get(extensionContext).getScraperName()).build());
        testStorage.addToTestStorage(Constants.SCRAPER_POD_KEY, kubeClient().listPodsByPrefixInName(storageMap.get(extensionContext).getNamespaceName(), storageMap.get(extensionContext).getScraperName()).get(0).getMetadata().getName());

        storageMap.put(extensionContext, testStorage);

        final KafkaTracingClientsBuilder kafkaTracingClientsBuilder = new KafkaTracingClientsBuilder()
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
            .withTracingServiceNameEnvVar(this.serviceNameEnvVar());
        final KafkaTracingClients kafkaTracingClient = (KafkaTracingClients) this.configureProperTracingType(kafkaTracingClientsBuilder.build());

        LOGGER.info("{}:\n", kafkaTracingClient.toString());

        testStorage.addToTestStorage(Constants.KAFKA_TRACING_CLIENT_KEY, kafkaTracingClient);

        storageMap.put(extensionContext, testStorage);
    }

    private KafkaClients configureProperTracingType(final KafkaClients kafkaClients) {
        if (kafkaClients instanceof BridgeTracingClients) {
            if (this.getClass().getSimpleName().contains(TracingConstants.OPEN_TELEMETRY)) {
                return new BridgeTracingClientsBuilder((BridgeTracingClients) kafkaClients).withOpenTelemetry().build();
            } else {
                throw new RuntimeException("Client " + kafkaClients.getClass().getSimpleName() + " + does not support tracing.");
            }
        } else if (kafkaClients instanceof KafkaTracingClients) {
            if (this.getClass().getSimpleName().contains(TracingConstants.OPEN_TELEMETRY)) {
                return new KafkaTracingClientsBuilder((KafkaTracingClients) kafkaClients).withOpenTelemetry().build();
            } else {
                throw new RuntimeException("Client " + kafkaClients.getClass().getSimpleName() + " + does not support tracing.");
            }
        } else {
            throw new RuntimeException("Client " + kafkaClients.getClass().getSimpleName() + " + does not support tracing.");
        }
    }

    @BeforeAll
    void setup(final ExtensionContext extensionContext) {
        this.clusterOperator = this.clusterOperator
                .defaultInstallation(extensionContext)
                .createInstallation()
                .runInstallation();

        ResourceManager.STORED_RESOURCES.computeIfAbsent(extensionContext.getDisplayName(), k -> new Stack<>());
        deployAndWaitForCertManager(extensionContext);
        deployJaegerOperator(extensionContext);
    }
}
