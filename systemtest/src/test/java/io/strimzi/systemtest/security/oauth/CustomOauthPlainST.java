/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.oauth;

import io.skodjob.testframe.MetricsCollector;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeResources;
import io.strimzi.api.kafka.model.common.InlineLogging;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.common.template.ContainerEnvVarBuilder;
import io.strimzi.api.kafka.model.common.template.ContainerEnvVarSourceBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpecBuilder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Resources;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.FIPSNotSupported;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClients;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClientsBuilder;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaOauthClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaOauthClientsBuilder;
import io.strimzi.systemtest.labels.LabelSelectors;
import io.strimzi.systemtest.metrics.KafkaBridgeMetricsComponent;
import io.strimzi.systemtest.metrics.KafkaConnectMetricsComponent;
import io.strimzi.systemtest.metrics.KafkaMetricsComponent;
import io.strimzi.systemtest.metrics.KafkaMirrorMaker2MetricsComponent;
import io.strimzi.systemtest.resources.crd.KafkaComponents;
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
import io.strimzi.systemtest.utils.kubeUtils.objects.NetworkPolicyUtils;
import io.strimzi.systemtest.utils.specific.MetricsUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.WaitException;
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

@Tag(OAUTH)
@Tag(REGRESSION)
@FIPSNotSupported("Keycloak is not customized to run on FIPS env - https://github.com/strimzi/strimzi-kafka-operator/issues/8331")
public class CustomOauthPlainST extends OauthAbstractST {
    protected static final Logger LOGGER = LogManager.getLogger(CustomOauthPlainST.class);

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
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        String producerName = OAUTH_PRODUCER_NAME + "-" + testStorage.getClusterName();
        String consumerName = OAUTH_CONSUMER_NAME + "-" + testStorage.getClusterName();

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), oauthClusterName).build());

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

        KubeResourceManager.get().createResourceWithWait(oauthExampleClients.producerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, producerName, testStorage.getMessageCount());

        KubeResourceManager.get().createResourceWithWait(oauthExampleClients.consumerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, consumerName, testStorage.getMessageCount());

        assertOauthMetricsForComponent(
            metricsCollector.toBuilder()
                .withComponent(KafkaMetricsComponent.create(oauthClusterName))
                .build()
        );
    }

    @ParallelTest
    void testSaslPlainProducerConsumer() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
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

        KubeResourceManager.get().createResourceWithWait(plainSaslOauthProducerClientsJob.producerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, audienceProducerName, testStorage.getMessageCount());

        KubeResourceManager.get().createResourceWithWait(plainSaslOauthConsumerClientsJob.consumerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, audienceConsumerName, testStorage.getMessageCount());
    }

    /**
     * As an OAuth KafkaConnect, I should be able to sink messages from kafka Broker Topic.
     */
    @ParallelTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    @Tag(METRICS)
    void testProducerConsumerConnectWithOauthMetrics() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
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

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), oauthClusterName).build());
        KubeResourceManager.get().createResourceWithWait(oauthExampleClients.producerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, producerName, testStorage.getMessageCount());

        KubeResourceManager.get().createResourceWithWait(oauthExampleClients.consumerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, consumerName, testStorage.getMessageCount());

        String connectJassConfig = JAAS_CONFIG_BUILDER.apply("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule", Map.of(
                "oauth.token.endpoint.uri", keycloakInstance.getOauthTokenEndpointUri(),
                "oauth.client.id", "kafka-connect",
                "oauth.client.secret", "${strimzienv:OAUTH_CLIENT_SECRET}",
                "oauth.connect.timeout.seconds", Integer.toString(CONNECT_TIMEOUT_S),
                "oauth.read.timeout.seconds", Integer.toString(READ_TIMEOUT_S),
                "oauth.enable.metrics", "true"
        ));

        KafkaConnect connect = KafkaConnectTemplates.kafkaConnectWithFilePlugin(Environment.TEST_SUITE_NAMESPACE, oauthClusterName, oauthClusterName, 1)
            .editOrNewSpec()
                .withReplicas(1)
                .withBootstrapServers(KafkaResources.plainBootstrapAddress(oauthClusterName))
                .withConfig(connectorConfig)
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .withNewKafkaClientAuthenticationCustom()
                    .withSasl(true)
                    .withConfig(Map.of(
                            "sasl.login.callback.handler.class", "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler",
                            "sasl.mechanism", "OAUTHBEARER",
                            "sasl.jaas.config", connectJassConfig
                    ))
                .endKafkaClientAuthenticationCustom()
                .withNewTemplate()
                    .withNewConnectContainer()
                        .withEnv(new ContainerEnvVarBuilder().withName("OAUTH_CLIENT_SECRET").withValueFrom(new ContainerEnvVarSourceBuilder().withNewSecretKeyRef(OAUTH_KEY, CONNECT_OAUTH_SECRET, false).build()).build())
                    .endConnectContainer()
                .endTemplate()
                .withMetricsConfig(OAUTH_METRICS)
            .endSpec()
            .build();
        // This is required to be able to remove the TLS setting, the builder cannot remove it
        connect.getSpec().setTls(null);

        KubeResourceManager.get().createResourceWithWait(connect);

        // Allow connections from scraper to Connect Pod when NetworkPolicies are set to denied by default
        NetworkPolicyUtils.allowNetworkPolicySettingsForResource(connect, KafkaConnectResources.componentName(oauthClusterName));

        final String kafkaConnectPodName = KubeResourceManager.get().kubeClient().listPods(Environment.TEST_SUITE_NAMESPACE,
                LabelSelectors.connectLabelSelector(oauthClusterName, KafkaConnectResources.componentName(oauthClusterName))).get(0).getMetadata().getName();

        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(Environment.TEST_SUITE_NAMESPACE, kafkaConnectPodName);

        KafkaConnectorUtils.createFileSinkConnector(Environment.TEST_SUITE_NAMESPACE, kafkaConnectPodName, testStorage.getTopicName(), TestConstants.DEFAULT_SINK_FILE_PATH, "http://localhost:8083");

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(Environment.TEST_SUITE_NAMESPACE, kafkaConnectPodName, TestConstants.DEFAULT_SINK_FILE_PATH, testStorage.getMessageCount());

        final String kafkaConnectLogs = KubeResourceManager.get().kubeCmdClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).logs(kafkaConnectPodName);
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
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
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

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), oauthClusterName).build());
        KubeResourceManager.get().createResourceWithWait(oauthExampleClients.producerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, producerName, testStorage.getMessageCount());

        KubeResourceManager.get().createResourceWithWait(oauthExampleClients.consumerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, consumerName, testStorage.getMessageCount());

        String kafkaSourceClusterName = oauthClusterName;

        String jaasConfig = JAAS_CONFIG_BUILDER.apply("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule",
                Map.of(
                        "unsecuredLoginStringClaim_sub", "thePrincipalName",
                        "oauth.valid.issuer.uri", keycloakInstance.getValidIssuerUri(),
                        "oauth.jwks.expiry.seconds", Integer.toString(keycloakInstance.getJwksExpireSeconds()),
                        "oauth.jwks.refresh.seconds", Integer.toString(keycloakInstance.getJwksRefreshSeconds()),
                        "oauth.jwks.endpoint.uri", keycloakInstance.getJwksEndpointUri(),
                        "oauth.username.claim", keycloakInstance.getUserNameClaim()
                ));

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetBrokerPoolName(), testStorage.getTargetClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetControllerPoolName(), testStorage.getTargetClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(Environment.TEST_SUITE_NAMESPACE, testStorage.getTargetClusterName(), 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                                    .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
                                    .withPort(9094)
                                    .withType(KafkaListenerType.NODEPORT)
                                    .withTls(false)
                                    .withNewKafkaListenerAuthenticationCustomAuth()
                                        .withSasl(true)
                                        .withListenerConfig(Map.of(
                                                "sasl.enabled.mechanisms", "OAUTHBEARER",
                                                "oauthbearer.sasl.server.callback.handler.class", "io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler",
                                                "oauthbearer.sasl.jaas.config", jaasConfig,
                                                "principal.builder.class", "io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder"
                                        ))
                                    .endKafkaListenerAuthenticationCustomAuth()
                                    .build(),
                                new GenericKafkaListenerBuilder()
                                    .withName(TestConstants.PLAIN_LISTENER_DEFAULT_NAME)
                                    .withPort(9092)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(false)
                                    .withNewKafkaListenerAuthenticationCustomAuth()
                                        .withSasl(true)
                                        .withListenerConfig(Map.of(
                                                "sasl.enabled.mechanisms", "OAUTHBEARER",
                                                "oauthbearer.sasl.server.callback.handler.class", "io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler",
                                                "oauthbearer.sasl.jaas.config", jaasConfig,
                                                "principal.builder.class", "io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder"
                                        ))
                                    .endKafkaListenerAuthenticationCustomAuth()
                                    .build())
                .endKafka()
            .endSpec()
            .build());

        // Deploy MirrorMaker2 with OAuth
        String sourceJassConfig = JAAS_CONFIG_BUILDER.apply("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule", Map.of(
                "oauth.token.endpoint.uri", keycloakInstance.getOauthTokenEndpointUri(),
                "oauth.client.id", "kafka-mirror-maker-2",
                "oauth.client.secret", "${strimzienv:OAUTH_CLIENT_SECRET}",
                "oauth.connect.timeout.seconds", Integer.toString(CONNECT_TIMEOUT_S),
                "oauth.read.timeout.seconds", Integer.toString(READ_TIMEOUT_S),
                "oauth.enable.metrics", "true"
        ));

        KafkaMirrorMaker2ClusterSpec sourceClusterWithOauth = new KafkaMirrorMaker2ClusterSpecBuilder()
                .withAlias(kafkaSourceClusterName)
                .withConfig(connectorConfig)
                .withBootstrapServers(KafkaResources.plainBootstrapAddress(kafkaSourceClusterName))
                .withNewKafkaClientAuthenticationCustom()
                    .withSasl(true)
                    .withConfig(Map.of(
                            "sasl.login.callback.handler.class", "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler",
                            "sasl.mechanism", "OAUTHBEARER",
                            "sasl.jaas.config", sourceJassConfig
                    ))
                .endKafkaClientAuthenticationCustom()
                .build();

        String targetJassConfig = JAAS_CONFIG_BUILDER.apply("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule", Map.of(
                "oauth.token.endpoint.uri", keycloakInstance.getOauthTokenEndpointUri(),
                "oauth.client.id", "kafka-mirror-maker-2",
                "oauth.client.secret", "${strimzienv:OAUTH_CLIENT_SECRET}",
                "oauth.connect.timeout.seconds", Integer.toString(CONNECT_TIMEOUT_S),
                "oauth.read.timeout.seconds", Integer.toString(READ_TIMEOUT_S),
                "oauth.enable.metrics", "true"
        ));
        KafkaMirrorMaker2ClusterSpec targetClusterWithOauth = new KafkaMirrorMaker2ClusterSpecBuilder()
                .withAlias(testStorage.getTargetClusterName())
                .withConfig(connectorConfig)
                .withBootstrapServers(KafkaResources.plainBootstrapAddress(testStorage.getTargetClusterName()))
                .withNewKafkaClientAuthenticationCustom()
                    .withSasl(true)
                    .withConfig(Map.of(
                            "sasl.login.callback.handler.class", "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler",
                            "sasl.mechanism", "OAUTHBEARER",
                            "sasl.jaas.config", targetJassConfig
                    ))
                .endKafkaClientAuthenticationCustom()
                .build();

        String kafkaTargetClusterTopicName = kafkaSourceClusterName + "." + testStorage.getTopicName();

        KubeResourceManager.get().createResourceWithWait(KafkaMirrorMaker2Templates.kafkaMirrorMaker2(Environment.TEST_SUITE_NAMESPACE, oauthClusterName, kafkaSourceClusterName, testStorage.getTargetClusterName(), 1, false)
            .editSpec()
                .withMetricsConfig(OAUTH_METRICS)
                .withClusters(sourceClusterWithOauth, targetClusterWithOauth)
                .editFirstMirror()
                    .withSourceCluster(kafkaSourceClusterName)
                .endMirror()
                .withNewTemplate()
                    .withNewConnectContainer()
                        .withEnv(new ContainerEnvVarBuilder().withName("OAUTH_CLIENT_SECRET").withValueFrom(new ContainerEnvVarSourceBuilder().withNewSecretKeyRef(OAUTH_KEY, MIRROR_MAKER_2_OAUTH_SECRET, false).build()).build())
                    .endConnectContainer()
                .endTemplate()
            .endSpec()
            .build());

        final String kafkaMirrorMaker2PodName = KubeResourceManager.get().kubeClient().listPods(Environment.TEST_SUITE_NAMESPACE,
            LabelSelectors.mirrorMaker2LabelSelector(oauthClusterName, KafkaMirrorMaker2Resources.componentName(oauthClusterName))).get(0).getMetadata().getName();
        final String kafkaMirrorMaker2Logs = KubeResourceManager.get().kubeCmdClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).logs(kafkaMirrorMaker2PodName);
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

                KubeResourceManager.get().createResourceWithWait(kafkaOauthClientJob.consumerStrimziOauthPlain());

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
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
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

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), oauthClusterName).build());
        KubeResourceManager.get().createResourceWithWait(oauthExampleClients.producerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, producerName, testStorage.getMessageCount());

        KubeResourceManager.get().createResourceWithWait(oauthExampleClients.consumerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, consumerName, testStorage.getMessageCount());

        // needed for a verification of oauth configuration
        InlineLogging ilDebug = new InlineLogging();
        ilDebug.setLoggers(Map.of("rootLogger.level", "DEBUG"));

        String bridgeJassConfig = JAAS_CONFIG_BUILDER.apply("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule", Map.of(
                "oauth.token.endpoint.uri", keycloakInstance.getOauthTokenEndpointUri(),
                "oauth.client.id", "kafka-bridge",
                "oauth.client.secret", "${strimzienv:OAUTH_CLIENT_SECRET}",
                "oauth.connect.timeout.seconds", Integer.toString(CONNECT_TIMEOUT_S),
                "oauth.read.timeout.seconds", Integer.toString(READ_TIMEOUT_S),
                "oauth.enable.metrics", "true"
        ));

        KubeResourceManager.get().createResourceWithWait(KafkaBridgeTemplates.kafkaBridgeWithMetrics(Environment.TEST_SUITE_NAMESPACE, oauthClusterName, KafkaResources.plainBootstrapAddress(oauthClusterName), 1)
            .editSpec()
                .withNewKafkaClientAuthenticationCustom()
                    .withSasl(true)
                    .withConfig(Map.of(
                            "sasl.login.callback.handler.class", "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler",
                            "sasl.mechanism", "OAUTHBEARER",
                            "sasl.jaas.config", bridgeJassConfig
                    ))
                .endKafkaClientAuthenticationCustom()
                .withNewTemplate()
                    .withNewBridgeContainer()
                        .withEnv(new ContainerEnvVarBuilder().withName("OAUTH_CLIENT_SECRET").withValueFrom(new ContainerEnvVarSourceBuilder().withNewSecretKeyRef(OAUTH_KEY, BRIDGE_OAUTH_SECRET, false).build()).build())
                    .endBridgeContainer()
                .endTemplate()
                .withLogging(ilDebug)
            .endSpec()
            .build());

        // Allow connections from scraper to Bridge pods when NetworkPolicies are set to denied by default
        NetworkPolicyUtils.allowNetworkPolicySettingsForBridgeScraper(Environment.TEST_SUITE_NAMESPACE, scraperPodName, KafkaBridgeResources.componentName(oauthClusterName));

        final String kafkaBridgePodName = KubeResourceManager.get().kubeClient().listPods(Environment.TEST_SUITE_NAMESPACE,
            LabelSelectors.bridgeLabelSelector(oauthClusterName, KafkaBridgeResources.componentName(oauthClusterName))).get(0).getMetadata().getName();
        final String kafkaBridgeLogs = KubeResourceManager.get().kubeCmdClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).logs(kafkaBridgePodName);
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

        KubeResourceManager.get().createResourceWithWait(kafkaBridgeClientJob.producerStrimziBridge());
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, bridgeProducerName, testStorage.getMessageCount());

        assertOauthMetricsForComponent(
            metricsCollector.toBuilder()
                .withComponent(KafkaBridgeMetricsComponent.create(Environment.TEST_SUITE_NAMESPACE, oauthClusterName))
                .build()
        );
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

        keycloakInstance.setRealm("internal", false);

        // Deploy OAuth metrics CM
        KubeResourceManager.get().kubeCmdClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).apply(FileUtils.updateNamespaceOfYamlFile(Environment.TEST_SUITE_NAMESPACE, OAUTH_METRICS_CM_PATH));

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(Environment.TEST_SUITE_NAMESPACE, KafkaComponents.getBrokerPoolName(oauthClusterName), oauthClusterName, 3).build(),
            KafkaNodePoolTemplates.controllerPool(Environment.TEST_SUITE_NAMESPACE, KafkaComponents.getControllerPoolName(oauthClusterName), oauthClusterName, 3).build()
        );

        String plainOauthbearerJaasConfig = JAAS_CONFIG_BUILDER.apply("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule",
                Map.of(
                        "unsecuredLoginStringClaim_sub", "thePrincipalName",
                        "oauth.valid.issuer.uri", keycloakInstance.getValidIssuerUri(),
                        "oauth.jwks.expiry.seconds", Integer.toString(keycloakInstance.getJwksExpireSeconds()),
                        "oauth.jwks.refresh.seconds", Integer.toString(keycloakInstance.getJwksRefreshSeconds()),
                        "oauth.jwks.endpoint.uri", keycloakInstance.getJwksEndpointUri(),
                        "oauth.username.claim", keycloakInstance.getUserNameClaim(),
                        "oauth.groups.claim", GROUPS_CLAIM,
                        "oauth.groups.claim.delimiter", GROUPS_CLAIM_DELIMITER,
                        "oauth.enable.metrics", "true"
                ));
        String plainPlainJaasConfig = JAAS_CONFIG_BUILDER.apply("org.apache.kafka.common.security.plain.PlainLoginModule",
                Map.of(
                        "oauth.valid.issuer.uri", keycloakInstance.getValidIssuerUri(),
                        "oauth.jwks.expiry.seconds", Integer.toString(keycloakInstance.getJwksExpireSeconds()),
                        "oauth.jwks.refresh.seconds", Integer.toString(keycloakInstance.getJwksRefreshSeconds()),
                        "oauth.jwks.endpoint.uri", keycloakInstance.getJwksEndpointUri(),
                        "oauth.username.claim", keycloakInstance.getUserNameClaim(),
                        "oauth.token.endpoint.uri", keycloakInstance.getOauthTokenEndpointUri(),
                        "oauth.groups.claim", GROUPS_CLAIM,
                        "oauth.groups.claim.delimiter", GROUPS_CLAIM_DELIMITER,
                        "oauth.enable.metrics", "true"
                ));

        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(Environment.TEST_SUITE_NAMESPACE, oauthClusterName, 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                                    .withName(TestConstants.PLAIN_LISTENER_DEFAULT_NAME)
                                    .withPort(9092)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(false)
                                    .withNewKafkaListenerAuthenticationCustomAuth()
                                        .withSasl(true)
                                        .withListenerConfig(Map.of(
                                                "sasl.enabled.mechanisms", "OAUTHBEARER,PLAIN",

                                                "oauthbearer.sasl.server.callback.handler.class", "io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler",
                                                "oauthbearer.sasl.jaas.config", plainOauthbearerJaasConfig,

                                                "plain.sasl.server.callback.handler.class", "io.strimzi.kafka.oauth.server.plain.JaasServerOauthOverPlainValidatorCallbackHandler",
                                                "plain.sasl.jaas.config", plainPlainJaasConfig,

                                                "principal.builder.class", "io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder"
                                        ))
                                    .endKafkaListenerAuthenticationCustomAuth()
                                    .build())
                    .withMetricsConfig(OAUTH_METRICS)
                .endKafka()
            .endSpec()
            .build());

        KubeResourceManager.get().createResourceWithWait(ScraperTemplates.scraperPod(Environment.TEST_SUITE_NAMESPACE, scraperName).build());
        scraperPodName = KubeResourceManager.get().kubeClient().listPodsByPrefixInName(Environment.TEST_SUITE_NAMESPACE, scraperName).get(0).getMetadata().getName();

        metricsCollector = new MetricsCollector.Builder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withScraperPodName(scraperPodName)
            .withComponent(KafkaMetricsComponent.create(oauthClusterName))
            .build();

        String brokerPodName = KubeResourceManager.get().kubeClient().listPods(Environment.TEST_SUITE_NAMESPACE,
            LabelSelectors.kafkaLabelSelector(oauthClusterName, KafkaComponents.getBrokerPodSetName(oauthClusterName))).get(0).getMetadata().getName();
        verifyOauthListenerConfiguration(KubeResourceManager.get().kubeClient().getLogsFromPod(Environment.TEST_SUITE_NAMESPACE, brokerPodName));
    }
}
