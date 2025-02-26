/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.oauth;

import io.skodjob.testframe.MetricsCollector;
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeResources;
import io.strimzi.api.kafka.model.common.InlineLogging;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpecBuilder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.FIPSNotSupported;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClients;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClientsBuilder;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaOauthClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaOauthClientsBuilder;
import io.strimzi.systemtest.metrics.KafkaBridgeMetricsComponent;
import io.strimzi.systemtest.metrics.KafkaConnectMetricsComponent;
import io.strimzi.systemtest.metrics.KafkaMetricsComponent;
import io.strimzi.systemtest.metrics.KafkaMirrorMaker2MetricsComponent;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.StrimziPodSetResource;
import io.strimzi.systemtest.resources.kubernetes.NetworkPolicyResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.FileUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.JobUtils;
import io.strimzi.systemtest.utils.specific.MetricsUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.WaitException;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.TestConstants.HTTP_BRIDGE_DEFAULT_PORT;
import static io.strimzi.systemtest.TestTags.BRIDGE;
import static io.strimzi.systemtest.TestTags.CONNECT;
import static io.strimzi.systemtest.TestTags.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.TestTags.METRICS;
import static io.strimzi.systemtest.TestTags.MIRROR_MAKER2;
import static io.strimzi.systemtest.TestTags.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.TestTags.OAUTH;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@Tag(OAUTH)
@Tag(REGRESSION)
@FIPSNotSupported("Keycloak is not customized to run on FIPS env - https://github.com/strimzi/strimzi-kafka-operator/issues/8331")
public class OauthPlainST extends OauthAbstractST {
    protected static final Logger LOGGER = LogManager.getLogger(OauthPlainST.class);

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

    /**
     * As an OAuth producer, I should be able to produce messages to the Kafka Broker,
     * As an OAuth consumer, I should be able to consumer messages from the Kafka Broker."
     */
    @ParallelTest
    @Tag(METRICS)
    void testProducerConsumerWithOauthMetrics() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        String producerName = OAUTH_PRODUCER_NAME + "-" + testStorage.getClusterName();
        String consumerName = OAUTH_CONSUMER_NAME + "-" + testStorage.getClusterName();

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), oauthClusterName).build());

        KafkaOauthClients oauthExampleClients = new KafkaOauthClientsBuilder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResourceWithWait(oauthExampleClients.producerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, producerName, testStorage.getMessageCount());

        resourceManager.createResourceWithWait(oauthExampleClients.consumerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, consumerName, testStorage.getMessageCount());

        assertOauthMetricsForComponent(
            metricsCollector.toBuilder()
                .withComponent(KafkaMetricsComponent.create(oauthClusterName))
                .build()
        );
    }

    @ParallelTest
    void testSaslPlainProducerConsumer() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        String audienceProducerName = OAUTH_CLIENT_AUDIENCE_PRODUCER + "-" + testStorage.getClusterName();
        String audienceConsumerName = OAUTH_CLIENT_AUDIENCE_CONSUMER + "-" + testStorage.getClusterName();

        String plainAdditionalConfig =
            "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=%s password=%s;\n" +
                "sasl.mechanism=PLAIN";

        KafkaOauthClients plainSaslOauthConsumerClientsJob = new KafkaOauthClientsBuilder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withConsumerName(audienceConsumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .withAdditionalConfig(String.format(plainAdditionalConfig, OAUTH_CLIENT_AUDIENCE_CONSUMER, OAUTH_CLIENT_AUDIENCE_SECRET))
            .build();

        KafkaOauthClients plainSaslOauthProducerClientsJob = new KafkaOauthClientsBuilder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withProducerName(audienceProducerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .withAdditionalConfig(String.format(plainAdditionalConfig, OAUTH_CLIENT_AUDIENCE_PRODUCER, OAUTH_CLIENT_AUDIENCE_SECRET))
            .build();

        resourceManager.createResourceWithWait(plainSaslOauthProducerClientsJob.producerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, audienceProducerName, testStorage.getMessageCount());

        resourceManager.createResourceWithWait(plainSaslOauthConsumerClientsJob.consumerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, audienceConsumerName, testStorage.getMessageCount());
    }

    @ParallelTest
    void testProducerConsumerAudienceTokenChecks() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        String producerName = OAUTH_PRODUCER_NAME + "-" + testStorage.getClusterName();
        String consumerName = OAUTH_CONSUMER_NAME + "-" + testStorage.getClusterName();
        String audienceProducerName = OAUTH_CLIENT_AUDIENCE_PRODUCER + "-" + testStorage.getClusterName();
        String audienceConsumerName = OAUTH_CLIENT_AUDIENCE_CONSUMER + "-" + testStorage.getClusterName();

        LOGGER.info("Setting producer and consumer properties");
        KafkaOauthClients oauthInternalClientJob = new KafkaOauthClientsBuilder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.bootstrapServiceName(oauthClusterName) + ":" + audienceListenerPort)
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        LOGGER.info("Use clients without access token containing audience token");
        resourceManager.createResourceWithWait(oauthInternalClientJob.producerStrimziOauthPlain());
        assertDoesNotThrow(() -> ClientUtils.waitForClientTimeout(Environment.TEST_SUITE_NAMESPACE, producerName, testStorage.getMessageCount()));
        resourceManager.createResourceWithWait(oauthInternalClientJob.consumerStrimziOauthPlain());
        assertDoesNotThrow(() -> ClientUtils.waitForClientTimeout(Environment.TEST_SUITE_NAMESPACE, consumerName, testStorage.getMessageCount()));

        JobUtils.deleteJobWithWait(Environment.TEST_SUITE_NAMESPACE, producerName);
        JobUtils.deleteJobWithWait(Environment.TEST_SUITE_NAMESPACE, consumerName);

        LOGGER.info("Use clients with Access token containing audience token");

        KafkaOauthClients oauthAudienceInternalClientJob = new KafkaOauthClientsBuilder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withProducerName(audienceProducerName)
            .withConsumerName(audienceConsumerName)
            .withBootstrapAddress(KafkaResources.bootstrapServiceName(oauthClusterName) + ":" + customClaimListenerPort)
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResourceWithWait(oauthAudienceInternalClientJob.producerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, audienceProducerName, testStorage.getMessageCount());

        resourceManager.createResourceWithWait(oauthAudienceInternalClientJob.consumerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, audienceConsumerName, testStorage.getMessageCount());
    }

    @ParallelTest
    void testAccessTokenClaimCheck() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        String producerName = OAUTH_PRODUCER_NAME + "-" + testStorage.getClusterName();
        String consumerName = OAUTH_CONSUMER_NAME + "-" + testStorage.getClusterName();
        String audienceProducerName = OAUTH_CLIENT_AUDIENCE_PRODUCER + "-" + testStorage.getClusterName();
        String audienceConsumerName = OAUTH_CLIENT_AUDIENCE_CONSUMER + "-" + testStorage.getClusterName();

        LOGGER.info("Use clients with clientId not containing 'hello-world' in access token");

        KafkaOauthClients oauthAudienceInternalClientJob = new KafkaOauthClientsBuilder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withProducerName(audienceProducerName)
            .withConsumerName(audienceConsumerName)
            .withBootstrapAddress(KafkaResources.bootstrapServiceName(oauthClusterName) + ":" + customClaimListenerPort)
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withOauthProducerClientId(OAUTH_CLIENT_AUDIENCE_PRODUCER)
            .withOauthConsumerClientId(OAUTH_CLIENT_AUDIENCE_CONSUMER)
            .withOauthClientSecret(OAUTH_CLIENT_AUDIENCE_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResourceWithWait(oauthAudienceInternalClientJob.producerStrimziOauthPlain());
        assertDoesNotThrow(() -> ClientUtils.waitForClientTimeout(Environment.TEST_SUITE_NAMESPACE, audienceProducerName, testStorage.getMessageCount()));
        resourceManager.createResourceWithWait(oauthAudienceInternalClientJob.consumerStrimziOauthPlain());
        assertDoesNotThrow(() -> ClientUtils.waitForClientTimeout(Environment.TEST_SUITE_NAMESPACE, audienceConsumerName, testStorage.getMessageCount()));

        JobUtils.deleteJobWithWait(Environment.TEST_SUITE_NAMESPACE, audienceProducerName);
        JobUtils.deleteJobWithWait(Environment.TEST_SUITE_NAMESPACE, audienceConsumerName);

        LOGGER.info("Use clients with clientId containing 'hello-world' in access token");

        KafkaOauthClients oauthInternalClientJob = new KafkaOauthClientsBuilder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.bootstrapServiceName(oauthClusterName) + ":" + customClaimListenerPort)
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResourceWithWait(oauthInternalClientJob.producerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, producerName, testStorage.getMessageCount());
        resourceManager.createResourceWithWait(oauthInternalClientJob.consumerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, consumerName, testStorage.getMessageCount());
    }

    /**
     * As an OAuth KafkaConnect, I should be able to sink messages from kafka Broker Topic.
     */
    @ParallelTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    @Tag(METRICS)
    void testProducerConsumerConnectWithOauthMetrics() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        String producerName = OAUTH_PRODUCER_NAME + "-" + testStorage.getClusterName();
        String consumerName = OAUTH_CONSUMER_NAME + "-" + testStorage.getClusterName();

        KafkaOauthClients oauthExampleClients = new KafkaOauthClientsBuilder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), oauthClusterName).build());
        resourceManager.createResourceWithWait(oauthExampleClients.producerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, producerName, testStorage.getMessageCount());

        resourceManager.createResourceWithWait(oauthExampleClients.consumerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, consumerName, testStorage.getMessageCount());

        KafkaConnect connect = KafkaConnectTemplates.kafkaConnectWithFilePlugin(Environment.TEST_SUITE_NAMESPACE, oauthClusterName, oauthClusterName, 1)
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
                .withMetricsConfig(OAUTH_METRICS)
            .endSpec()
            .build();
        // This is required to be able to remove the TLS setting, the builder cannot remove it
        connect.getSpec().setTls(null);

        resourceManager.createResourceWithWait(connect);

        // Allow connections from scraper to Connect Pod when NetworkPolicies are set to denied by default
        NetworkPolicyResource.allowNetworkPolicySettingsForResource(connect, KafkaConnectResources.componentName(oauthClusterName));

        final String kafkaConnectPodName = kubeClient().listPods(Environment.TEST_SUITE_NAMESPACE, oauthClusterName, Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();

        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(Environment.TEST_SUITE_NAMESPACE, kafkaConnectPodName);

        KafkaConnectorUtils.createFileSinkConnector(Environment.TEST_SUITE_NAMESPACE, kafkaConnectPodName, testStorage.getTopicName(), TestConstants.DEFAULT_SINK_FILE_PATH, "http://localhost:8083");

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(Environment.TEST_SUITE_NAMESPACE, kafkaConnectPodName, TestConstants.DEFAULT_SINK_FILE_PATH, testStorage.getMessageCount());

        final String kafkaConnectLogs = KubeClusterResource.cmdKubeClient(Environment.TEST_SUITE_NAMESPACE).execInCurrentNamespace(Level.DEBUG, "logs", kafkaConnectPodName).out();
        verifyOauthConfiguration(kafkaConnectLogs);

        assertOauthMetricsForComponent(
            metricsCollector.toBuilder()
                .withComponent(KafkaConnectMetricsComponent.create(oauthClusterName))
                .build()
        );
    }

    @IsolatedTest("Using more than one Kafka cluster in one Namespace")
    @Tag(MIRROR_MAKER2)
    @Tag(CONNECT_COMPONENTS)
    @Tag(NODEPORT_SUPPORTED)
    @Tag(METRICS)
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testProducerConsumerMirrorMaker2WithOauthMetrics() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        String producerName = OAUTH_PRODUCER_NAME + "-" + testStorage.getClusterName();
        String consumerName = OAUTH_CONSUMER_NAME + "-" + testStorage.getClusterName();

        KafkaOauthClients oauthExampleClients = new KafkaOauthClientsBuilder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), oauthClusterName).build());
        resourceManager.createResourceWithWait(oauthExampleClients.producerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, producerName, testStorage.getMessageCount());

        resourceManager.createResourceWithWait(oauthExampleClients.consumerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, consumerName, testStorage.getMessageCount());

        String kafkaSourceClusterName = oauthClusterName;

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetBrokerPoolName(), testStorage.getTargetClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetControllerPoolName(), testStorage.getTargetClusterName(), 1).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(Environment.TEST_SUITE_NAMESPACE, testStorage.getTargetClusterName(), 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                                .withName(TestConstants.PLAIN_LISTENER_DEFAULT_NAME)
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
                                .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
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

        // Deploy MirrorMaker2 with OAuth
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
            .withAlias(testStorage.getTargetClusterName())
            .withConfig(connectorConfig)
            .withBootstrapServers(KafkaResources.plainBootstrapAddress(testStorage.getTargetClusterName()))
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

        String kafkaTargetClusterTopicName = kafkaSourceClusterName + "." + testStorage.getTopicName();

        resourceManager.createResourceWithWait(KafkaMirrorMaker2Templates.kafkaMirrorMaker2(Environment.TEST_SUITE_NAMESPACE, oauthClusterName, kafkaSourceClusterName, testStorage.getTargetClusterName(), 1, false)
            .editSpec()
                .withMetricsConfig(OAUTH_METRICS)
                .withClusters(sourceClusterWithOauth, targetClusterWithOauth)
                .editFirstMirror()
                    .withSourceCluster(kafkaSourceClusterName)
                .endMirror()
            .endSpec()
            .build());

        final String kafkaMirrorMaker2PodName = kubeClient().listPods(Environment.TEST_SUITE_NAMESPACE, oauthClusterName, Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND).get(0).getMetadata().getName();
        final String kafkaMirrorMaker2Logs = KubeClusterResource.cmdKubeClient(Environment.TEST_SUITE_NAMESPACE).execInCurrentNamespace(Level.DEBUG, "logs", kafkaMirrorMaker2PodName).out();
        verifyOauthConfiguration(kafkaMirrorMaker2Logs);

        TestUtils.waitFor("MirrorMaker2 to copy messages from " + kafkaSourceClusterName + " to " + testStorage.getTargetClusterName(),
            Duration.ofSeconds(30).toMillis(), TestConstants.TIMEOUT_FOR_MIRROR_MAKER_2_COPY_MESSAGES_BETWEEN_BROKERS,
            () -> {
                LOGGER.info("Deleting Job: {}/{}", Environment.TEST_SUITE_NAMESPACE, consumerName);
                JobUtils.deleteJobWithWait(Environment.TEST_SUITE_NAMESPACE, consumerName);

                LOGGER.info("Creating new client with new consumer-group and also to point on {} cluster", testStorage.getTargetClusterName());

                KafkaOauthClients kafkaOauthClientJob = new KafkaOauthClientsBuilder()
                    .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
                    .withProducerName(producerName)
                    .withConsumerName(consumerName)
                    .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getTargetClusterName()))
                    .withTopicName(kafkaTargetClusterTopicName)
                    .withMessageCount(testStorage.getMessageCount())
                    .withOauthClientId(OAUTH_CLIENT_NAME)
                    .withOauthClientSecret(OAUTH_CLIENT_SECRET)
                    .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                    .build();

                resourceManager.createResourceWithWait(kafkaOauthClientJob.consumerStrimziOauthPlain());

                try {
                    ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, consumerName, testStorage.getMessageCount());
                    return  true;
                } catch (WaitException e) {
                    e.printStackTrace();
                    return false;
                }
            });

        assertOauthMetricsForComponent(
            metricsCollector.toBuilder()
                .withComponent(KafkaMirrorMaker2MetricsComponent.create(oauthClusterName))
                .build()
        );
    }

    /**
     * As a OAuth bridge, I should be able to send messages to bridge endpoint.
     */
    @ParallelTest
    @Tag(BRIDGE)
    @Tag(METRICS)
    void testProducerConsumerBridgeWithOauthMetrics() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        String producerName = OAUTH_PRODUCER_NAME + "-" + testStorage.getClusterName();
        String consumerName = OAUTH_CONSUMER_NAME + "-" + testStorage.getClusterName();

        KafkaOauthClients oauthExampleClients = new KafkaOauthClientsBuilder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), oauthClusterName).build());
        resourceManager.createResourceWithWait(oauthExampleClients.producerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, producerName, testStorage.getMessageCount());

        resourceManager.createResourceWithWait(oauthExampleClients.consumerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, consumerName, testStorage.getMessageCount());

        // needed for a verification of oauth configuration
        InlineLogging ilDebug = new InlineLogging();
        ilDebug.setLoggers(Map.of("rootLogger.level", "DEBUG"));

        resourceManager.createResourceWithWait(KafkaBridgeTemplates.kafkaBridgeWithMetrics(Environment.TEST_SUITE_NAMESPACE, oauthClusterName, KafkaResources.plainBootstrapAddress(oauthClusterName), 1)
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

        // Allow connections from scraper to Bridge pods when NetworkPolicies are set to denied by default
        NetworkPolicyResource.allowNetworkPolicySettingsForBridgeScraper(Environment.TEST_SUITE_NAMESPACE, scraperPodName, KafkaBridgeResources.componentName(oauthClusterName));

        final String kafkaBridgePodName = kubeClient().listPods(Environment.TEST_SUITE_NAMESPACE, oauthClusterName, Labels.STRIMZI_KIND_LABEL, KafkaBridge.RESOURCE_KIND).get(0).getMetadata().getName();
        final String kafkaBridgeLogs = KubeClusterResource.cmdKubeClient(Environment.TEST_SUITE_NAMESPACE).execInCurrentNamespace(Level.DEBUG, "logs", kafkaBridgePodName).out();
        verifyOauthConfiguration(kafkaBridgeLogs);

        String bridgeProducerName = "bridge-producer-" + testStorage.getClusterName();

        BridgeClients kafkaBridgeClientJob = new BridgeClientsBuilder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withProducerName(bridgeProducerName)
            .withBootstrapAddress(KafkaBridgeResources.serviceName(oauthClusterName))
            .withComponentName(KafkaBridgeResources.componentName(oauthClusterName))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withPort(HTTP_BRIDGE_DEFAULT_PORT)
            .withDelayMs(1000)
            .withPollInterval(1000)
            .build();

        resourceManager.createResourceWithWait(kafkaBridgeClientJob.producerStrimziBridge());
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, bridgeProducerName, testStorage.getMessageCount());

        assertOauthMetricsForComponent(
            metricsCollector.toBuilder()
                .withComponent(KafkaBridgeMetricsComponent.create(Environment.TEST_SUITE_NAMESPACE, oauthClusterName))
                .build()
        );
    }

    @ParallelTest
    void testSaslPlainAuthenticationKafkaConnectIsAbleToConnectToKafkaOAuth() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithoutWait(KafkaConnectTemplates.kafkaConnect(testStorage.getNamespaceName(), testStorage.getClusterName(), oauthClusterName, 1)
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
        LOGGER.info("Checking OAuth metrics for component: {}", collector.toString());
        collector.collectMetricsFromPods(TestConstants.METRICS_COLLECT_TIMEOUT);

        for (final String podName : collector.getCollectedData().keySet()) {
            for (final String expectedMetric : expectedOauthMetrics) {
                LOGGER.info("Searching value from Pod with IP {} for metric {}", podName, expectedMetric);
                MetricsUtils.assertContainsMetric(collector.getCollectedData().get(podName), expectedMetric);
            }
        }
    }

    @BeforeAll
    void setUp() throws Exception {
        super.setupCoAndKeycloak(Environment.TEST_SUITE_NAMESPACE);

        final String customClaimListener = "cclistener";
        final String audienceListener = "audlistnr";

        keycloakInstance.setRealm("internal", false);

        // Deploy OAuth metrics CM
        cmdKubeClient(Environment.TEST_SUITE_NAMESPACE).apply(FileUtils.updateNamespaceOfYamlFile(Environment.TEST_SUITE_NAMESPACE, OAUTH_METRICS_CM_PATH));

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(Environment.TEST_SUITE_NAMESPACE, KafkaNodePoolResource.getBrokerPoolName(oauthClusterName), oauthClusterName, 3).build(),
            KafkaNodePoolTemplates.controllerPool(Environment.TEST_SUITE_NAMESPACE, KafkaNodePoolResource.getControllerPoolName(oauthClusterName), oauthClusterName, 3).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(Environment.TEST_SUITE_NAMESPACE, oauthClusterName, 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                                .withName(TestConstants.PLAIN_LISTENER_DEFAULT_NAME)
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
                                    .withCustomClaimCheck("@.client_id && @.client_id =~ /.*hello-world.*/")
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
                    .withMetricsConfig(OAUTH_METRICS)
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResourceWithWait(ScraperTemplates.scraperPod(Environment.TEST_SUITE_NAMESPACE, scraperName).build());
        scraperPodName = kubeClient().listPodsByPrefixInName(Environment.TEST_SUITE_NAMESPACE, scraperName).get(0).getMetadata().getName();

        metricsCollector = new MetricsCollector.Builder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withScraperPodName(scraperPodName)
            .withComponent(KafkaMetricsComponent.create(oauthClusterName))
            .build();

        String brokerPodName = kubeClient().listPods(Environment.TEST_SUITE_NAMESPACE,
            KafkaResource.getLabelSelector(oauthClusterName, StrimziPodSetResource.getBrokerComponentName(oauthClusterName))).get(0).getMetadata().getName();
        verifyOauthListenerConfiguration(kubeClient().logsInSpecificNamespace(Environment.TEST_SUITE_NAMESPACE, brokerPodName));
    }
}
