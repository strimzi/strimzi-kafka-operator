/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.oauth;

import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2ClusterSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2ClusterSpecBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.IsolatedSuite;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClients;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClientsBuilder;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaOauthClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaOauthClientsBuilder;
import io.strimzi.systemtest.metrics.MetricsCollector;
import io.strimzi.systemtest.resources.ComponentType;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMakerTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.FileUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.JobUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.WaitException;
import io.strimzi.test.k8s.KubeClusterResource;
import io.vertx.core.cli.annotations.Description;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.HTTP_BRIDGE_DEFAULT_PORT;
import static io.strimzi.systemtest.Constants.METRICS;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER2;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.OAUTH;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@Tag(OAUTH)
@Tag(REGRESSION)
@IsolatedSuite
public class OauthPlainIsolatedST extends OauthAbstractST {
    protected static final Logger LOGGER = LogManager.getLogger(OauthPlainIsolatedST.class);

    private static final String OAUTH_METRICS_CM_PATH = TestUtils.USER_PATH + "/../packaging/examples/metrics/oauth-metrics.yaml";
    private static final String OAUTH_METRICS_CM_KEY = "metrics-config.yml";
    private static final String OAUTH_METRICS_CM_NAME = "oauth-metrics";

    private static final JmxPrometheusExporterMetrics OAUTH_METRICS =
        new JmxPrometheusExporterMetricsBuilder()
            .withNewValueFrom()
                .withNewConfigMapKeyRef(OAUTH_METRICS_CM_KEY, OAUTH_METRICS_CM_NAME, false)
            .endValueFrom()
            .build();

    private final String oauthClusterName = "oauth-cluster-plain-name";
    private final String scraperName = "oauth-cluster-plain-scraper";
    private String scraperPodName = "";
    private final String customClaimListenerPort = "9099";
    private final List<String> expectedOauthMetrics = Arrays.asList(
        "strimzi_oauth_http_requests_maxtimems", "strimzi_oauth_http_requests_mintimems",
        "strimzi_oauth_http_requests_avgtimems", "strimzi_oauth_http_requests_totaltimems",
        "strimzi_oauth_http_requests_count"
    );

    private MetricsCollector metricsCollector;

    @Description(
            "As an oauth producer, I should be able to produce messages to the kafka broker\n" +
            "As an oauth consumer, I should be able to consumer messages from the kafka broker.")
    @ParallelTest
    @Tag(METRICS)
    void testProducerConsumerWithOauthMetrics(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String producerName = OAUTH_PRODUCER_NAME + "-" + clusterName;
        String consumerName = OAUTH_CONSUMER_NAME + "-" + clusterName;
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, topicName, clusterOperator.getDeploymentNamespace()).build());

        KafkaOauthClients oauthExampleClients = new KafkaOauthClientsBuilder()
            .withNamespaceName(clusterOperator.getDeploymentNamespace())
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResource(extensionContext, oauthExampleClients.producerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(producerName, clusterOperator.getDeploymentNamespace(), MESSAGE_COUNT);

        resourceManager.createResource(extensionContext, oauthExampleClients.consumerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(consumerName, clusterOperator.getDeploymentNamespace(), MESSAGE_COUNT);

        assertOauthMetricsForComponent(
            metricsCollector.toBuilder()
                .withComponentType(ComponentType.Kafka)
                .build()
        );
    }

    @ParallelTest
    void testSaslPlainProducerConsumer(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String audienceProducerName = OAUTH_CLIENT_AUDIENCE_PRODUCER + "-" + clusterName;
        String audienceConsumerName = OAUTH_CLIENT_AUDIENCE_CONSUMER + "-" + clusterName;
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        String plainAdditionalConfig =
            "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=%s password=%s;\n" +
                "sasl.mechanism=PLAIN";

        KafkaOauthClients plainSaslOauthConsumerClientsJob = new KafkaOauthClientsBuilder()
            .withNamespaceName(clusterOperator.getDeploymentNamespace())
            .withConsumerName(audienceConsumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .withAdditionalConfig(String.format(plainAdditionalConfig, OAUTH_CLIENT_AUDIENCE_CONSUMER, OAUTH_CLIENT_AUDIENCE_SECRET))
            .build();

        KafkaOauthClients plainSaslOauthProducerClientsJob = new KafkaOauthClientsBuilder()
            .withNamespaceName(clusterOperator.getDeploymentNamespace())
            .withProducerName(audienceProducerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .withAdditionalConfig(String.format(plainAdditionalConfig, OAUTH_CLIENT_AUDIENCE_PRODUCER, OAUTH_CLIENT_AUDIENCE_SECRET))
            .build();

        resourceManager.createResource(extensionContext, plainSaslOauthProducerClientsJob.producerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(audienceProducerName, clusterOperator.getDeploymentNamespace(), MESSAGE_COUNT);

        resourceManager.createResource(extensionContext, plainSaslOauthConsumerClientsJob.consumerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(audienceConsumerName, clusterOperator.getDeploymentNamespace(), MESSAGE_COUNT);
    }

    @ParallelTest
    void testProducerConsumerAudienceTokenChecks(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String producerName = OAUTH_PRODUCER_NAME + "-" + clusterName;
        String consumerName = OAUTH_CONSUMER_NAME + "-" + clusterName;
        String audienceProducerName = OAUTH_CLIENT_AUDIENCE_PRODUCER + "-" + clusterName;
        String audienceConsumerName = OAUTH_CLIENT_AUDIENCE_CONSUMER + "-" + clusterName;
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        LOGGER.info("Setting producer and consumer properties");
        KafkaOauthClients oauthInternalClientJob = new KafkaOauthClientsBuilder()
            .withNamespaceName(clusterOperator.getDeploymentNamespace())
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.bootstrapServiceName(oauthClusterName) + ":" + audienceListenerPort)
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        LOGGER.info("Use clients without access token containing audience token");
        resourceManager.createResource(extensionContext, oauthInternalClientJob.producerStrimziOauthPlain());
        assertDoesNotThrow(() -> ClientUtils.waitForClientTimeout(producerName, clusterOperator.getDeploymentNamespace(), MESSAGE_COUNT));
        resourceManager.createResource(extensionContext, oauthInternalClientJob.consumerStrimziOauthPlain());
        assertDoesNotThrow(() -> ClientUtils.waitForClientTimeout(consumerName, clusterOperator.getDeploymentNamespace(), MESSAGE_COUNT));

        JobUtils.deleteJobWithWait(clusterOperator.getDeploymentNamespace(), producerName);
        JobUtils.deleteJobWithWait(clusterOperator.getDeploymentNamespace(), consumerName);

        LOGGER.info("Use clients with Access token containing audience token");

        KafkaOauthClients oauthAudienceInternalClientJob = new KafkaOauthClientsBuilder()
            .withNamespaceName(clusterOperator.getDeploymentNamespace())
            .withProducerName(audienceProducerName)
            .withConsumerName(audienceConsumerName)
            .withBootstrapAddress(KafkaResources.bootstrapServiceName(oauthClusterName) + ":" + customClaimListenerPort)
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResource(extensionContext, oauthAudienceInternalClientJob.producerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(audienceProducerName, clusterOperator.getDeploymentNamespace(), MESSAGE_COUNT);

        resourceManager.createResource(extensionContext, oauthAudienceInternalClientJob.consumerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(audienceConsumerName, clusterOperator.getDeploymentNamespace(), MESSAGE_COUNT);
    }

    @ParallelTest
    void testAccessTokenClaimCheck(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String producerName = OAUTH_PRODUCER_NAME + "-" + clusterName;
        String consumerName = OAUTH_CONSUMER_NAME + "-" + clusterName;
        String audienceProducerName = OAUTH_CLIENT_AUDIENCE_PRODUCER + "-" + clusterName;
        String audienceConsumerName = OAUTH_CLIENT_AUDIENCE_CONSUMER + "-" + clusterName;
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        LOGGER.info("Use clients with clientId not containing 'hello-world' in access token.");

        KafkaOauthClients oauthAudienceInternalClientJob = new KafkaOauthClientsBuilder()
            .withNamespaceName(clusterOperator.getDeploymentNamespace())
            .withProducerName(audienceProducerName)
            .withConsumerName(audienceConsumerName)
            .withBootstrapAddress(KafkaResources.bootstrapServiceName(oauthClusterName) + ":" + customClaimListenerPort)
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withOauthProducerClientId(OAUTH_CLIENT_AUDIENCE_PRODUCER)
            .withOauthConsumerClientId(OAUTH_CLIENT_AUDIENCE_CONSUMER)
            .withOauthClientSecret(OAUTH_CLIENT_AUDIENCE_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResource(extensionContext, oauthAudienceInternalClientJob.producerStrimziOauthPlain());
        assertDoesNotThrow(() -> ClientUtils.waitForClientTimeout(audienceProducerName, clusterOperator.getDeploymentNamespace(), MESSAGE_COUNT));
        resourceManager.createResource(extensionContext, oauthAudienceInternalClientJob.consumerStrimziOauthPlain());
        assertDoesNotThrow(() -> ClientUtils.waitForClientTimeout(audienceConsumerName, clusterOperator.getDeploymentNamespace(), MESSAGE_COUNT));

        JobUtils.deleteJobWithWait(clusterOperator.getDeploymentNamespace(), audienceProducerName);
        JobUtils.deleteJobWithWait(clusterOperator.getDeploymentNamespace(), audienceConsumerName);

        LOGGER.info("Use clients with clientId containing 'hello-world' in access token.");

        KafkaOauthClients oauthInternalClientJob = new KafkaOauthClientsBuilder()
            .withNamespaceName(clusterOperator.getDeploymentNamespace())
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.bootstrapServiceName(oauthClusterName) + ":" + customClaimListenerPort)
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResource(extensionContext, oauthInternalClientJob.producerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(producerName, clusterOperator.getDeploymentNamespace(), MESSAGE_COUNT);
        resourceManager.createResource(extensionContext, oauthInternalClientJob.consumerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(consumerName, clusterOperator.getDeploymentNamespace(), MESSAGE_COUNT);
    }

    @Description("As an oauth KafkaConnect, I should be able to sink messages from kafka broker topic.")
    @ParallelTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    @Tag(METRICS)
    void testProducerConsumerConnectWithOauthMetrics(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String producerName = OAUTH_PRODUCER_NAME + "-" + clusterName;
        String consumerName = OAUTH_CONSUMER_NAME + "-" + clusterName;
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        KafkaOauthClients oauthExampleClients = new KafkaOauthClientsBuilder()
            .withNamespaceName(clusterOperator.getDeploymentNamespace())
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, topicName, clusterOperator.getDeploymentNamespace()).build());
        resourceManager.createResource(extensionContext, oauthExampleClients.producerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(producerName, clusterOperator.getDeploymentNamespace(), MESSAGE_COUNT);

        resourceManager.createResource(extensionContext, oauthExampleClients.consumerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(consumerName, clusterOperator.getDeploymentNamespace(), MESSAGE_COUNT);

        KafkaConnect connect = KafkaConnectTemplates.kafkaConnectWithFilePlugin(oauthClusterName, clusterOperator.getDeploymentNamespace(), oauthClusterName, 1)
            .editMetadata()
                .withNamespace(clusterOperator.getDeploymentNamespace())
            .endMetadata()
            .editOrNewSpec()
                .withReplicas(1)
                .withBootstrapServers(KafkaResources.plainBootstrapAddress(oauthClusterName))
                .withConfig(connectorConfig)
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .withNewKafkaClientAuthenticationOAuth()
                    .withEnableMetrics()
                    .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                    .withClientId("kafka-connect")
                    .withNewClientSecret()
                        .withSecretName(CONNECT_OAUTH_SECRET)
                        .withKey(OAUTH_KEY)
                    .endClientSecret()
                    .withConnectTimeoutSeconds(CONNECT_TIMEOUT_S)
                    .withReadTimeoutSeconds(READ_TIMEOUT_S)
                .endKafkaClientAuthenticationOAuth()
                .withNewInlineLogging()
                    // needed for a verification of oauth configuration
                    .addToLoggers("connect.root.logger.level", "DEBUG")
                .endInlineLogging()
                .withJmxPrometheusExporterMetricsConfig(OAUTH_METRICS)
            .endSpec()
            .build();
        // This is required to be able to remove the TLS setting, the builder cannot remove it
        connect.getSpec().setTls(null);

        resourceManager.createResource(extensionContext, connect);

        final String kafkaConnectPodName = kubeClient().listPods(clusterOperator.getDeploymentNamespace(), oauthClusterName, Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();

        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(clusterOperator.getDeploymentNamespace(), kafkaConnectPodName);

        KafkaConnectorUtils.createFileSinkConnector(clusterOperator.getDeploymentNamespace(), kafkaConnectPodName, topicName, Constants.DEFAULT_SINK_FILE_PATH, "http://localhost:8083");

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(clusterOperator.getDeploymentNamespace(), kafkaConnectPodName, Constants.DEFAULT_SINK_FILE_PATH, "\"Hello-world - 99\"");

        final String kafkaConnectLogs = KubeClusterResource.cmdKubeClient(clusterOperator.getDeploymentNamespace()).execInCurrentNamespace(Level.DEBUG, "logs", kafkaConnectPodName).out();
        verifyOauthConfiguration(kafkaConnectLogs);

        assertOauthMetricsForComponent(
            metricsCollector.toBuilder()
                .withComponentType(ComponentType.KafkaConnect)
                .build()
        );
    }

    @Description("As an oauth mirror maker, I should be able to replicate topic data between kafka clusters")
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(MIRROR_MAKER)
    @Tag(NODEPORT_SUPPORTED)
    void testProducerConsumerMirrorMaker(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String producerName = OAUTH_PRODUCER_NAME + "-" + clusterName;
        String consumerName = OAUTH_CONSUMER_NAME + "-" + clusterName;
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        KafkaOauthClients oauthExampleClients = new KafkaOauthClientsBuilder()
            .withNamespaceName(clusterOperator.getDeploymentNamespace())
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, topicName, clusterOperator.getDeploymentNamespace()).build());
        resourceManager.createResource(extensionContext, oauthExampleClients.producerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(producerName, clusterOperator.getDeploymentNamespace(), MESSAGE_COUNT);

        resourceManager.createResource(extensionContext, oauthExampleClients.consumerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(consumerName, clusterOperator.getDeploymentNamespace(), MESSAGE_COUNT);

        String targetKafkaCluster = clusterName + "-target";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(targetKafkaCluster, 1, 1)
            .editMetadata()
                .withNamespace(clusterOperator.getDeploymentNamespace())
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                                .withName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .withNewKafkaListenerAuthenticationOAuth()
                                    .withValidIssuerUri(keycloakInstance.getValidIssuerUri())
                                    .withJwksEndpointUri(keycloakInstance.getJwksEndpointUri())
                                    .withJwksExpirySeconds(keycloakInstance.getJwksExpireSeconds())
                                    .withJwksRefreshSeconds(keycloakInstance.getJwksRefreshSeconds())
                                    .withUserNameClaim(keycloakInstance.getUserNameClaim())
                                .endKafkaListenerAuthenticationOAuth()
                                .build(),
                            new GenericKafkaListenerBuilder()
                                .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(false)
                                .withNewKafkaListenerAuthenticationOAuth()
                                    .withValidIssuerUri(keycloakInstance.getValidIssuerUri())
                                    .withJwksExpirySeconds(keycloakInstance.getJwksExpireSeconds())
                                    .withJwksRefreshSeconds(keycloakInstance.getJwksRefreshSeconds())
                                    .withJwksEndpointUri(keycloakInstance.getJwksEndpointUri())
                                    .withUserNameClaim(keycloakInstance.getUserNameClaim())
                                .endKafkaListenerAuthenticationOAuth()
                                .build())
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(oauthClusterName, oauthClusterName, targetKafkaCluster,
            ClientUtils.generateRandomConsumerGroup(), 1, false)
                .editMetadata()
                    .withNamespace(clusterOperator.getDeploymentNamespace())
                .endMetadata()
                .editSpec()
                    .withNewConsumer()
                        .withBootstrapServers(KafkaResources.plainBootstrapAddress(oauthClusterName))
                        .withGroupId(ClientUtils.generateRandomConsumerGroup())
                        .addToConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                        .withNewKafkaClientAuthenticationOAuth()
                            .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                            .withClientId("kafka-mirror-maker")
                            .withNewClientSecret()
                                .withSecretName(MIRROR_MAKER_OAUTH_SECRET)
                                .withKey(OAUTH_KEY)
                            .endClientSecret()
                            .withConnectTimeoutSeconds(CONNECT_TIMEOUT_S)
                            .withReadTimeoutSeconds(READ_TIMEOUT_S)
                        .endKafkaClientAuthenticationOAuth()
                        .withTls(null)
                    .endConsumer()
                    .withNewProducer()
                        .withBootstrapServers(KafkaResources.plainBootstrapAddress(targetKafkaCluster))
                        .withNewKafkaClientAuthenticationOAuth()
                            .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                            .withClientId("kafka-mirror-maker")
                            .withNewClientSecret()
                                .withSecretName(MIRROR_MAKER_OAUTH_SECRET)
                                .withKey(OAUTH_KEY)
                            .endClientSecret()
                            .withConnectTimeoutSeconds(CONNECT_TIMEOUT_S)
                            .withReadTimeoutSeconds(READ_TIMEOUT_S)
                        .endKafkaClientAuthenticationOAuth()
                        .addToConfig(ProducerConfig.ACKS_CONFIG, "all")
                        .withTls(null)
                    .endProducer()
                .endSpec()
                .build());

        final String kafkaMirrorMakerPodName = kubeClient().listPods(clusterOperator.getDeploymentNamespace(), oauthClusterName, Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker.RESOURCE_KIND).get(0).getMetadata().getName();
        final String kafkaMirrorMakerLogs = KubeClusterResource.cmdKubeClient(clusterOperator.getDeploymentNamespace()).execInCurrentNamespace(Level.DEBUG, "logs", kafkaMirrorMakerPodName).out();
        verifyOauthConfiguration(kafkaMirrorMakerLogs);

        TestUtils.waitFor("Waiting for Mirror Maker will copy messages from " + oauthClusterName + " to " + targetKafkaCluster,
            Constants.GLOBAL_CLIENTS_POLL, Constants.TIMEOUT_FOR_MIRROR_MAKER_COPY_MESSAGES_BETWEEN_BROKERS,
            () -> {
                LOGGER.info("Deleting the Job");
                JobUtils.deleteJobWithWait(clusterOperator.getDeploymentNamespace(), OAUTH_CONSUMER_NAME);

                LOGGER.info("Creating new client with new consumer-group and also to point on {} cluster", targetKafkaCluster);
                KafkaOauthClients kafkaOauthClientJob = new KafkaOauthClientsBuilder()
                    .withNamespaceName(clusterOperator.getDeploymentNamespace())
                    .withProducerName(consumerName)
                    .withConsumerName(OAUTH_CONSUMER_NAME)
                    .withBootstrapAddress(KafkaResources.plainBootstrapAddress(targetKafkaCluster))
                    .withTopicName(topicName)
                    .withMessageCount(MESSAGE_COUNT)
                    .withOauthClientId(OAUTH_CLIENT_NAME)
                    .withOauthClientSecret(OAUTH_CLIENT_SECRET)
                    .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                    .build();

                resourceManager.createResource(extensionContext, kafkaOauthClientJob.consumerStrimziOauthPlain());

                try {
                    ClientUtils.waitForClientSuccess(OAUTH_CONSUMER_NAME, clusterOperator.getDeploymentNamespace(), MESSAGE_COUNT);
                    return  true;
                } catch (WaitException e) {
                    e.printStackTrace();
                    return false;
                }
            });
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(MIRROR_MAKER2)
    @Tag(CONNECT_COMPONENTS)
    @Tag(NODEPORT_SUPPORTED)
    @Tag(METRICS)
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testProducerConsumerMirrorMaker2WithOauthMetrics(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String producerName = OAUTH_PRODUCER_NAME + "-" + clusterName;
        String consumerName = OAUTH_CONSUMER_NAME + "-" + clusterName;
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        KafkaOauthClients oauthExampleClients = new KafkaOauthClientsBuilder()
            .withNamespaceName(clusterOperator.getDeploymentNamespace())
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, topicName, clusterOperator.getDeploymentNamespace()).build());
        resourceManager.createResource(extensionContext, oauthExampleClients.producerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(producerName, clusterOperator.getDeploymentNamespace(), MESSAGE_COUNT);

        resourceManager.createResource(extensionContext, oauthExampleClients.consumerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(consumerName, clusterOperator.getDeploymentNamespace(), MESSAGE_COUNT);

        String kafkaSourceClusterName = oauthClusterName;
        String kafkaTargetClusterName = clusterName + "-target";
        // mirror maker 2 adding prefix to mirrored topic for in this case mirrotopic will be : my-cluster.my-topic
        String kafkaTargetClusterTopicName = kafkaSourceClusterName + "." + topicName;

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaTargetClusterName, 1, 1)
            .editMetadata()
                .withNamespace(clusterOperator.getDeploymentNamespace())
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                                .withName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .withNewKafkaListenerAuthenticationOAuth()
                                    .withValidIssuerUri(keycloakInstance.getValidIssuerUri())
                                    .withJwksEndpointUri(keycloakInstance.getJwksEndpointUri())
                                    .withJwksExpirySeconds(keycloakInstance.getJwksExpireSeconds())
                                    .withJwksRefreshSeconds(keycloakInstance.getJwksRefreshSeconds())
                                    .withUserNameClaim(keycloakInstance.getUserNameClaim())
                                .endKafkaListenerAuthenticationOAuth()
                                .build(),
                            new GenericKafkaListenerBuilder()
                                .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(false)
                                .withNewKafkaListenerAuthenticationOAuth()
                                    .withValidIssuerUri(keycloakInstance.getValidIssuerUri())
                                    .withJwksExpirySeconds(keycloakInstance.getJwksExpireSeconds())
                                    .withJwksRefreshSeconds(keycloakInstance.getJwksRefreshSeconds())
                                    .withJwksEndpointUri(keycloakInstance.getJwksEndpointUri())
                                    .withUserNameClaim(keycloakInstance.getUserNameClaim())
                                .endKafkaListenerAuthenticationOAuth()
                                .build())
                .endKafka()
            .endSpec()
            .build());

        // Deploy Mirror Maker 2.0 with oauth
        KafkaMirrorMaker2ClusterSpec sourceClusterWithOauth = new KafkaMirrorMaker2ClusterSpecBuilder()
            .withAlias(kafkaSourceClusterName)
            .withConfig(connectorConfig)
            .withBootstrapServers(KafkaResources.plainBootstrapAddress(kafkaSourceClusterName))
            .withNewKafkaClientAuthenticationOAuth()
                .withEnableMetrics()
                .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                .withClientId("kafka-mirror-maker-2")
                .withNewClientSecret()
                    .withSecretName(MIRROR_MAKER_2_OAUTH_SECRET)
                    .withKey(OAUTH_KEY)
                .endClientSecret()
                .withConnectTimeoutSeconds(CONNECT_TIMEOUT_S)
                .withReadTimeoutSeconds(READ_TIMEOUT_S)
            .endKafkaClientAuthenticationOAuth()
            .build();

        KafkaMirrorMaker2ClusterSpec targetClusterWithOauth = new KafkaMirrorMaker2ClusterSpecBuilder()
            .withAlias(kafkaTargetClusterName)
            .withConfig(connectorConfig)
            .withBootstrapServers(KafkaResources.plainBootstrapAddress(kafkaTargetClusterName))
            .withNewKafkaClientAuthenticationOAuth()
                .withEnableMetrics()
                .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                .withClientId("kafka-mirror-maker-2")
                .withNewClientSecret()
                    .withSecretName(MIRROR_MAKER_2_OAUTH_SECRET)
                    .withKey(OAUTH_KEY)
                .endClientSecret()
                .withConnectTimeoutSeconds(CONNECT_TIMEOUT_S)
                .withReadTimeoutSeconds(READ_TIMEOUT_S)
            .endKafkaClientAuthenticationOAuth()
            .build();

        resourceManager.createResource(extensionContext, KafkaMirrorMaker2Templates.kafkaMirrorMaker2(oauthClusterName, kafkaTargetClusterName, kafkaSourceClusterName, 1, false)
            .editMetadata()
                .withNamespace(clusterOperator.getDeploymentNamespace())
            .endMetadata()
            .editSpec()
                .withJmxPrometheusExporterMetricsConfig(OAUTH_METRICS)
                .withClusters(sourceClusterWithOauth, targetClusterWithOauth)
                .editFirstMirror()
                    .withSourceCluster(kafkaSourceClusterName)
                .endMirror()
            .endSpec()
            .build());

        final String kafkaMirrorMaker2PodName = kubeClient().listPods(clusterOperator.getDeploymentNamespace(), oauthClusterName, Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND).get(0).getMetadata().getName();
        final String kafkaMirrorMaker2Logs = KubeClusterResource.cmdKubeClient(clusterOperator.getDeploymentNamespace()).execInCurrentNamespace(Level.DEBUG, "logs", kafkaMirrorMaker2PodName).out();
        verifyOauthConfiguration(kafkaMirrorMaker2Logs);

        TestUtils.waitFor("Waiting for Mirror Maker 2 will copy messages from " + kafkaSourceClusterName + " to " + kafkaTargetClusterName,
            Duration.ofSeconds(30).toMillis(), Constants.TIMEOUT_FOR_MIRROR_MAKER_COPY_MESSAGES_BETWEEN_BROKERS,
            () -> {
                LOGGER.info("Deleting the Job {}", consumerName);
                JobUtils.deleteJobWithWait(clusterOperator.getDeploymentNamespace(), consumerName);

                LOGGER.info("Creating new client with new consumer-group and also to point on {} cluster", kafkaTargetClusterName);

                KafkaOauthClients kafkaOauthClientJob = new KafkaOauthClientsBuilder()
                    .withNamespaceName(clusterOperator.getDeploymentNamespace())
                    .withProducerName(producerName)
                    .withConsumerName(consumerName)
                    .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaTargetClusterName))
                    .withTopicName(kafkaTargetClusterTopicName)
                    .withMessageCount(MESSAGE_COUNT)
                    .withOauthClientId(OAUTH_CLIENT_NAME)
                    .withOauthClientSecret(OAUTH_CLIENT_SECRET)
                    .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                    .build();

                resourceManager.createResource(extensionContext, kafkaOauthClientJob.consumerStrimziOauthPlain());

                try {
                    ClientUtils.waitForClientSuccess(consumerName, clusterOperator.getDeploymentNamespace(), MESSAGE_COUNT);
                    return  true;
                } catch (WaitException e) {
                    e.printStackTrace();
                    return false;
                }
            });

        assertOauthMetricsForComponent(
            metricsCollector.toBuilder()
                .withComponentType(ComponentType.KafkaMirrorMaker2)
                .build()
        );
    }

    @Description("As a oauth bridge, I should be able to send messages to bridge endpoint.")
    @ParallelTest
    @Tag(BRIDGE)
    @Tag(METRICS)
    void testProducerConsumerBridgeWithOauthMetrics(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String producerName = OAUTH_PRODUCER_NAME + "-" + clusterName;
        String consumerName = OAUTH_CONSUMER_NAME + "-" + clusterName;
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        KafkaOauthClients oauthExampleClients = new KafkaOauthClientsBuilder()
            .withNamespaceName(clusterOperator.getDeploymentNamespace())
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, topicName, clusterOperator.getDeploymentNamespace()).build());
        resourceManager.createResource(extensionContext, oauthExampleClients.producerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(producerName, clusterOperator.getDeploymentNamespace(), MESSAGE_COUNT);

        resourceManager.createResource(extensionContext, oauthExampleClients.consumerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(consumerName, clusterOperator.getDeploymentNamespace(), MESSAGE_COUNT);

        // needed for a verification of oauth configuration
        InlineLogging ilDebug = new InlineLogging();
        ilDebug.setLoggers(Map.of("rootLogger.level", "DEBUG"));

        resourceManager.createResource(extensionContext, KafkaBridgeTemplates.kafkaBridgeWithMetrics(oauthClusterName, oauthClusterName, KafkaResources.plainBootstrapAddress(oauthClusterName), 1)
            .editMetadata()
                .withNamespace(clusterOperator.getDeploymentNamespace())
            .endMetadata()
            .editSpec()
                .withNewKafkaClientAuthenticationOAuth()
                    .withEnableMetrics()
                    .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                    .withClientId("kafka-bridge")
                    .withNewClientSecret()
                        .withSecretName(BRIDGE_OAUTH_SECRET)
                        .withKey(OAUTH_KEY)
                    .endClientSecret()
                    .withConnectTimeoutSeconds(CONNECT_TIMEOUT_S)
                    .withReadTimeoutSeconds(READ_TIMEOUT_S)
                .endKafkaClientAuthenticationOAuth()
                .withLogging(ilDebug)
            .endSpec()
            .build());

        final String kafkaBridgePodName = kubeClient().listPods(clusterOperator.getDeploymentNamespace(), oauthClusterName, Labels.STRIMZI_KIND_LABEL, KafkaBridge.RESOURCE_KIND).get(0).getMetadata().getName();
        final String kafkaBridgeLogs = KubeClusterResource.cmdKubeClient(clusterOperator.getDeploymentNamespace()).execInCurrentNamespace(Level.DEBUG, "logs", kafkaBridgePodName).out();
        verifyOauthConfiguration(kafkaBridgeLogs);

        String bridgeProducerName = "bridge-producer-" + clusterName;

        BridgeClients kafkaBridgeClientJob = new BridgeClientsBuilder()
            .withNamespaceName(clusterOperator.getDeploymentNamespace())
            .withProducerName(bridgeProducerName)
            .withBootstrapAddress(KafkaBridgeResources.serviceName(oauthClusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withPort(HTTP_BRIDGE_DEFAULT_PORT)
            .withDelayMs(1000)
            .withPollInterval(1000)
            .build();

        resourceManager.createResource(extensionContext, kafkaBridgeClientJob.producerStrimziBridge());
        ClientUtils.waitForClientSuccess(bridgeProducerName, clusterOperator.getDeploymentNamespace(), MESSAGE_COUNT);

        assertOauthMetricsForComponent(
            metricsCollector.toBuilder()
                .withComponentType(ComponentType.KafkaBridge)
                .build()
        );
    }

    @ParallelTest
    void testSaslPlainAuthenticationKafkaConnectIsAbleToConnectToKafkaOAuth(ExtensionContext extensionContext) {
        TestStorage testStorage = new TestStorage(extensionContext);

        resourceManager.createResource(extensionContext, false, KafkaConnectTemplates.kafkaConnect(testStorage.getClusterName(), testStorage.getNamespaceName(), oauthClusterName, 1)
            .withNewSpec()
                .withReplicas(1)
                .withBootstrapServers(KafkaResources.plainBootstrapAddress(oauthClusterName))
                .withConfig(connectorConfig)
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .withNewKafkaClientAuthenticationPlain()
                    .withUsername("kafka-connect")
                    .withNewPasswordSecret()
                        .withSecretName(CONNECT_OAUTH_SECRET)
                        .withPassword("clientSecret")
                    .endPasswordSecret()
                .endKafkaClientAuthenticationPlain()
                .withTls(null)
            .endSpec()
            .build());

        // verify that KafkaConnect is able to connect to Oauth Kafka configured as plain
        KafkaConnectUtils.waitForConnectReady(testStorage.getNamespaceName(), testStorage.getClusterName());
    }

    private void assertOauthMetricsForComponent(MetricsCollector collector) {
        LOGGER.info("Checking OAuth metrics for component: {} with name: {}", collector.getComponentType(), collector.getComponentName());
        collector.collectMetricsFromPods();

        for (final String podName : collector.getCollectedData().keySet()) {
            for (final String expectedMetric : expectedOauthMetrics) {
                LOGGER.info("Searching value from pod with IP {} for metric {}", podName, expectedMetric);
                assertThat(collector.getCollectedData().get(podName), containsString(expectedMetric));
            }
        }
    }

    @BeforeAll
    void setUp(ExtensionContext extensionContext) throws Exception {
        super.setupCoAndKeycloak(extensionContext, clusterOperator.getDeploymentNamespace());

        final String customClaimListener = "cclistener";
        final String audienceListener = "audlistnr";

        keycloakInstance.setRealm("internal", false);

        // Deploy OAuth metrics CM
        cmdKubeClient().apply(FileUtils.updateNamespaceOfYamlFile(OAUTH_METRICS_CM_PATH, clusterOperator.getDeploymentNamespace()));

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(oauthClusterName, 3, 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                                .withName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .withNewKafkaListenerAuthenticationOAuth()
                                    .withValidIssuerUri(keycloakInstance.getValidIssuerUri())
                                    .withJwksExpirySeconds(keycloakInstance.getJwksExpireSeconds())
                                    .withJwksRefreshSeconds(keycloakInstance.getJwksRefreshSeconds())
                                    .withJwksEndpointUri(keycloakInstance.getJwksEndpointUri())
                                    .withUserNameClaim(keycloakInstance.getUserNameClaim())
                                    .withEnablePlain(true)
                                    .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                                    .withGroupsClaim(GROUPS_CLAIM)
                                    .withGroupsClaimDelimiter(GROUPS_CLAIM_DELIMITER)
                                    .withEnableMetrics()
                                .endKafkaListenerAuthenticationOAuth()
                                .build(),
                            new GenericKafkaListenerBuilder()
                                .withName(customClaimListener)
                                .withPort(Integer.parseInt(customClaimListenerPort))
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .withNewKafkaListenerAuthenticationOAuth()
                                    .withValidIssuerUri(keycloakInstance.getValidIssuerUri())
                                    .withJwksExpirySeconds(keycloakInstance.getJwksExpireSeconds())
                                    .withJwksRefreshSeconds(keycloakInstance.getJwksRefreshSeconds())
                                    .withJwksEndpointUri(keycloakInstance.getJwksEndpointUri())
                                    .withUserNameClaim(keycloakInstance.getUserNameClaim())
                                    .withEnablePlain(true)
                                    .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                                    .withCustomClaimCheck("@.clientId && @.clientId =~ /.*hello-world.*/")
                                .endKafkaListenerAuthenticationOAuth()
                                .build(),
                            new GenericKafkaListenerBuilder()
                                .withName(audienceListener)
                                .withPort(Integer.parseInt(audienceListenerPort))
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .withNewKafkaListenerAuthenticationOAuth()
                                    .withValidIssuerUri(keycloakInstance.getValidIssuerUri())
                                    .withJwksExpirySeconds(keycloakInstance.getJwksExpireSeconds())
                                    .withJwksRefreshSeconds(keycloakInstance.getJwksRefreshSeconds())
                                    .withJwksEndpointUri(keycloakInstance.getJwksEndpointUri())
                                    .withUserNameClaim(keycloakInstance.getUserNameClaim())
                                    .withEnablePlain(true)
                                    .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                                    .withCheckAudience(true)
                                    .withClientId("kafka-component")
                                .endKafkaListenerAuthenticationOAuth()
                                .build())
                    .withJmxPrometheusExporterMetricsConfig(OAUTH_METRICS)
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, ScraperTemplates.scraperPod(clusterOperator.getDeploymentNamespace(), scraperName).build());
        scraperPodName = kubeClient().listPodsByPrefixInName(clusterOperator.getDeploymentNamespace(), scraperName).get(0).getMetadata().getName();

        metricsCollector = new MetricsCollector.Builder()
            .withNamespaceName(clusterOperator.getDeploymentNamespace())
            .withScraperPodName(scraperPodName)
            .withComponentName(oauthClusterName)
            .withComponentType(ComponentType.Kafka)
            .build();

        verifyOauthListenerConfiguration(kubeClient().logsInSpecificNamespace(clusterOperator.getDeploymentNamespace(), KafkaResources.kafkaPodName(oauthClusterName, 0)));
    }
}
