/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.oauth;

import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeResources;
import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.api.kafka.model.common.CertSecretSourceBuilder;
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
import io.strimzi.systemtest.keycloak.KeycloakInstance;
import io.strimzi.systemtest.labels.LabelSelectors;
import io.strimzi.systemtest.resources.crd.KafkaComponents;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.JobUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.NetworkPolicyUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.WaitException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.time.Duration;

import static io.strimzi.systemtest.TestConstants.HTTP_BRIDGE_DEFAULT_PORT;
import static io.strimzi.systemtest.TestTags.ACCEPTANCE;
import static io.strimzi.systemtest.TestTags.BRIDGE;
import static io.strimzi.systemtest.TestTags.CONNECT;
import static io.strimzi.systemtest.TestTags.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.TestTags.MIRROR_MAKER2;
import static io.strimzi.systemtest.TestTags.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.TestTags.OAUTH;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@Tag(OAUTH)
@Tag(REGRESSION)
@FIPSNotSupported("Keycloak is not customized to run on FIPS env - https://github.com/strimzi/strimzi-kafka-operator/issues/8331")
public class OauthTlsST extends OauthAbstractST {
    protected static final Logger LOGGER = LogManager.getLogger(OauthTlsST.class);

    private final String oauthClusterName = "oauth-cluster-tls-name";

    /**
     * As an OAuth producer, I am able to produce messages to the Kafka Brokers,
     * As an OAuth consumer, I am able to consumer messages from the Kafka Broker using encrypted communication
     */
    @ParallelTest
    void testProducerConsumer() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        String producerName = OAUTH_PRODUCER_NAME + "-" + testStorage.getClusterName();
        String consumerName = OAUTH_CONSUMER_NAME + "-" + testStorage.getClusterName();

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), oauthClusterName).build());

        KafkaOauthClients oauthExampleClients = new KafkaOauthClientsBuilder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(oauthClusterName))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        KubeResourceManager.get().createResourceWithWait(oauthExampleClients.producerStrimziOauthTls(oauthClusterName));
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, producerName, testStorage.getMessageCount());

        KubeResourceManager.get().createResourceWithWait(oauthExampleClients.consumerStrimziOauthTls(oauthClusterName));
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, consumerName, testStorage.getMessageCount());
    }

    /**
     * As an OAuth KafkaConnect, I am able to sink messages from Kafka Broker topic using encrypted communication.
     */
    @ParallelTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    @Tag(ACCEPTANCE)
    void testProducerConsumerConnect() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        String producerName = OAUTH_PRODUCER_NAME + "-" + testStorage.getClusterName();
        String consumerName = OAUTH_CONSUMER_NAME + "-" + testStorage.getClusterName();

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), oauthClusterName).build());

        KafkaOauthClients oauthExampleClients = new KafkaOauthClientsBuilder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(oauthClusterName))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        KubeResourceManager.get().createResourceWithWait(oauthExampleClients.producerStrimziOauthTls(oauthClusterName));
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, producerName, testStorage.getMessageCount());

        KubeResourceManager.get().createResourceWithWait(oauthExampleClients.consumerStrimziOauthTls(oauthClusterName));
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, consumerName, testStorage.getMessageCount());

        KafkaConnect connect = KafkaConnectTemplates.kafkaConnectWithFilePlugin(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), oauthClusterName, 1)
            .editSpec()
                .withConfig(connectorConfig)
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .withNewKafkaClientAuthenticationOAuth()
                    .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                    .withClientId("kafka-connect")
                    .withNewClientSecret()
                    .withSecretName("my-connect-oauth")
                    .withKey(OAUTH_KEY)
                    .endClientSecret()
                    .withTlsTrustedCertificates(
                        new CertSecretSourceBuilder()
                            .withSecretName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                            .withCertificate(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                            .build())
                    .withDisableTlsHostnameVerification(true)
                .endKafkaClientAuthenticationOAuth()
                .withNewTls()
                    .addNewTrustedCertificate()
                        .withSecretName(oauthClusterName + "-cluster-ca-cert")
                        .withCertificate("ca.crt")
                    .endTrustedCertificate()
                .endTls()
                .withBootstrapServers(oauthClusterName + "-kafka-bootstrap:9093")
            .endSpec()
            .build();

        KubeResourceManager.get().createResourceWithWait(connect, ScraperTemplates.scraperPod(Environment.TEST_SUITE_NAMESPACE, testStorage.getScraperName()).build());

        LOGGER.info("Deploying NetworkPolicies for KafkaConnect");
        NetworkPolicyUtils.deployNetworkPolicyForResource(connect, KafkaConnectResources.componentName(testStorage.getClusterName()));

        String kafkaConnectPodName = KubeResourceManager.get().kubeClient().listPods(Environment.TEST_SUITE_NAMESPACE, testStorage.getKafkaConnectSelector()).get(0).getMetadata().getName();
        String scraperPodName = KubeResourceManager.get().kubeClient().listPodsByPrefixInName(Environment.TEST_SUITE_NAMESPACE, testStorage.getScraperName()).get(0).getMetadata().getName();
        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(Environment.TEST_SUITE_NAMESPACE, kafkaConnectPodName);

        KafkaConnectorUtils.createFileSinkConnector(Environment.TEST_SUITE_NAMESPACE, scraperPodName, testStorage.getTopicName(), TestConstants.DEFAULT_SINK_FILE_PATH, KafkaConnectResources.url(testStorage.getClusterName(), Environment.TEST_SUITE_NAMESPACE, 8083));

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(Environment.TEST_SUITE_NAMESPACE, kafkaConnectPodName, TestConstants.DEFAULT_SINK_FILE_PATH, testStorage.getMessageCount());
    }

    /**
     * As a OAuth bridge, i am able to send messages to bridge endpoint using encrypted communication
     */
    @ParallelTest
    @Tag(BRIDGE)
    @Tag(ACCEPTANCE)
    void testProducerConsumerBridge() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        String producerName = OAUTH_PRODUCER_NAME + "-" + testStorage.getClusterName();
        String consumerName = OAUTH_CONSUMER_NAME + "-" + testStorage.getClusterName();

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), oauthClusterName).build());

        KafkaOauthClients oauthExampleClients = new KafkaOauthClientsBuilder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(oauthClusterName))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        KubeResourceManager.get().createResourceWithWait(oauthExampleClients.producerStrimziOauthTls(oauthClusterName));
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, producerName, testStorage.getMessageCount());

        KubeResourceManager.get().createResourceWithWait(oauthExampleClients.consumerStrimziOauthTls(oauthClusterName));
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, consumerName, testStorage.getMessageCount());

        KubeResourceManager.get().createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(Environment.TEST_SUITE_NAMESPACE, oauthClusterName, KafkaResources.tlsBootstrapAddress(oauthClusterName), 1)
            .editSpec()
                .withNewTls()
                    .withTrustedCertificates(
                        new CertSecretSourceBuilder()
                            .withCertificate("ca.crt")
                            .withSecretName(KafkaResources.clusterCaCertificateSecretName(oauthClusterName)).build())
                .endTls()
                .withNewKafkaClientAuthenticationOAuth()
                    .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                    .withClientId("kafka-bridge")
                    .withNewClientSecret()
                        .withSecretName(BRIDGE_OAUTH_SECRET)
                        .withKey(OAUTH_KEY)
                    .endClientSecret()
                    .addNewTlsTrustedCertificate()
                        .withSecretName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                        .withCertificate(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                    .endTlsTrustedCertificate()
                    .withDisableTlsHostnameVerification(true)
                .endKafkaClientAuthenticationOAuth()
            .endSpec()
            .build());

        producerName = "bridge-producer-" + testStorage.getClusterName();

        BridgeClients kafkaBridgeClientJob = new BridgeClientsBuilder()
            .withProducerName(producerName)
            .withBootstrapAddress(KafkaBridgeResources.serviceName(oauthClusterName))
            .withComponentName(KafkaBridgeResources.componentName(oauthClusterName))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(10)
            .withPort(HTTP_BRIDGE_DEFAULT_PORT)
            .withDelayMs(1000)
            .withPollInterval(1000)
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .build();

        KubeResourceManager.get().createResourceWithWait(kafkaBridgeClientJob.producerStrimziBridge());
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, producerName, testStorage.getMessageCount());
    }

    /**
     * As a OAuth MirrorMaker 2, I am able to replicate Topic data using encrypted communication
     */
    @IsolatedTest("Using more tha one Kafka cluster in one Namespace")
    @Tag(MIRROR_MAKER2)
    @Tag(NODEPORT_SUPPORTED)
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testMirrorMaker2() {
        // Nodeport needs cluster wide rights to work properly which is not possible with STRIMZI_RBAC_SCOPE=NAMESPACE
        assumeFalse(Environment.isNamespaceRbacScope());
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        String producerName = OAUTH_PRODUCER_NAME + "-" + testStorage.getClusterName();
        String consumerName = OAUTH_CONSUMER_NAME + "-" + testStorage.getClusterName();

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), oauthClusterName).build());

        KafkaOauthClients oauthExampleClients = new KafkaOauthClientsBuilder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(oauthClusterName))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        KubeResourceManager.get().createResourceWithWait(oauthExampleClients.producerStrimziOauthTls(oauthClusterName));
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, producerName, testStorage.getMessageCount());

        KubeResourceManager.get().createResourceWithWait(oauthExampleClients.consumerStrimziOauthTls(oauthClusterName));
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, consumerName, testStorage.getMessageCount());

        String targetKafkaCluster = oauthClusterName + "-target";
        String kafkaSourceClusterName = oauthClusterName;

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getTargetBrokerPoolName(), targetKafkaCluster, 1).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getTargetControllerPoolName(), targetKafkaCluster, 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(Environment.TEST_SUITE_NAMESPACE, targetKafkaCluster, 1)
            .editSpec()
                .editKafka()
                    .withListeners(OauthAbstractST.BUILD_OAUTH_TLS_LISTENER.apply(keycloakInstance),
                            new GenericKafkaListenerBuilder()
                                .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withNewKafkaListenerAuthenticationOAuth()
                                .withValidIssuerUri(keycloakInstance.getValidIssuerUri())
                                .withJwksExpirySeconds(keycloakInstance.getJwksExpireSeconds())
                                .withJwksRefreshSeconds(keycloakInstance.getJwksRefreshSeconds())
                                .withJwksEndpointUri(keycloakInstance.getJwksEndpointUri())
                                .withUserNameClaim(keycloakInstance.getUserNameClaim())
                                .withTlsTrustedCertificates(
                                    new CertSecretSourceBuilder()
                                        .withSecretName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                                        .withCertificate(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                                        .build())
                                    .withDisableTlsHostnameVerification(true)
                                .endKafkaListenerAuthenticationOAuth()
                                .build())
                .endKafka()
            .endSpec()
            .build());

        // Deploy MirrorMaker2 with OAuth
        KafkaMirrorMaker2ClusterSpec sourceClusterWithOauth = new KafkaMirrorMaker2ClusterSpecBuilder()
            .withAlias(kafkaSourceClusterName)
            .withConfig(connectorConfig)
            .withBootstrapServers(KafkaResources.tlsBootstrapAddress(oauthClusterName))
            // this is for kafka tls connection
            .withNewTls()
                .withTrustedCertificates(new CertSecretSourceBuilder()
                    .withCertificate("ca.crt")
                    .withSecretName(KafkaResources.clusterCaCertificateSecretName(oauthClusterName))
                    .build())
            .endTls()
            .withNewKafkaClientAuthenticationOAuth()
                .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                .withClientId("kafka-mirror-maker-2")
                .withNewClientSecret()
                    .withSecretName(MIRROR_MAKER_2_OAUTH_SECRET)
                    .withKey(OAUTH_KEY)
                .endClientSecret()
                .withConnectTimeoutSeconds(CONNECT_TIMEOUT_S)
                .withReadTimeoutSeconds(READ_TIMEOUT_S)
                // this is for authorization server tls connection
                .withTlsTrustedCertificates(new CertSecretSourceBuilder()
                    .withSecretName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                    .withCertificate(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                    .build())
                .withDisableTlsHostnameVerification(true)
            .endKafkaClientAuthenticationOAuth()
            .build();

        KafkaMirrorMaker2ClusterSpec targetClusterWithOauth = new KafkaMirrorMaker2ClusterSpecBuilder()
            .withAlias(testStorage.getTargetClusterName())
            .withConfig(connectorConfig)
            .withBootstrapServers(KafkaResources.tlsBootstrapAddress(targetKafkaCluster))
            // this is for kafka tls connection (using pattern)
            .withNewTls()
                .withTrustedCertificates(new CertSecretSourceBuilder()
                    .withPattern("*.crt")
                    .withSecretName(KafkaResources.clusterCaCertificateSecretName(targetKafkaCluster))
                    .build())
            .endTls()
            .withNewKafkaClientAuthenticationOAuth()
                .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                .withClientId("kafka-mirror-maker-2")
                .withNewClientSecret()
                    .withSecretName(MIRROR_MAKER_2_OAUTH_SECRET)
                    .withKey(OAUTH_KEY)
                .endClientSecret()
                .withConnectTimeoutSeconds(CONNECT_TIMEOUT_S)
                .withReadTimeoutSeconds(READ_TIMEOUT_S)
                // this is for authorization server tls connection
                .withTlsTrustedCertificates(new CertSecretSourceBuilder()
                    .withSecretName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                    .withCertificate(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                    .build())
                .withDisableTlsHostnameVerification(true)
            .endKafkaClientAuthenticationOAuth()
            .build();

        String kafkaTargetClusterTopicName = kafkaSourceClusterName + "." + testStorage.getTopicName();

        KubeResourceManager.get().createResourceWithWait(KafkaMirrorMaker2Templates.kafkaMirrorMaker2(Environment.TEST_SUITE_NAMESPACE, oauthClusterName, kafkaSourceClusterName, testStorage.getTargetClusterName(), 1, false)
            .editSpec()
                .withClusters(sourceClusterWithOauth, targetClusterWithOauth)
                .editFirstMirror()
                    .withSourceCluster(kafkaSourceClusterName)
                .endMirror()
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
                            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(targetKafkaCluster))
                            .withTopicName(kafkaTargetClusterTopicName)
                            .withMessageCount(testStorage.getMessageCount())
                            .withOauthClientId(OAUTH_CLIENT_NAME)
                            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
                            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                            .build();

                    KubeResourceManager.get().createResourceWithWait(kafkaOauthClientJob.consumerStrimziOauthTls(targetKafkaCluster));

                    try {
                        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, consumerName, testStorage.getMessageCount());
                        return  true;
                    } catch (WaitException e) {
                        LOGGER.error("Failed while waiting for consumer to succeed", e);
                        return false;
                    }
                });
    }

    @ParallelTest
    void testIntrospectionEndpoint() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        String producerName = OAUTH_PRODUCER_NAME + "-" + testStorage.getClusterName();
        String consumerName = OAUTH_CONSUMER_NAME + "-" + testStorage.getClusterName();

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), oauthClusterName).build());

        keycloakInstance.setIntrospectionEndpointUri("https://" + keycloakInstance.getHttpsUri() + "/realms/internal/protocol/openid-connect/token/introspect");
        String introspectionKafka = oauthClusterName + "-intro";

        CertSecretSource cert = new CertSecretSourceBuilder()
                .withSecretName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                .withCertificate(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                .build();

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(Environment.TEST_SUITE_NAMESPACE, KafkaComponents.getBrokerPoolName(introspectionKafka), introspectionKafka, 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(Environment.TEST_SUITE_NAMESPACE, KafkaComponents.getControllerPoolName(introspectionKafka), introspectionKafka, 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(Environment.TEST_SUITE_NAMESPACE, introspectionKafka, 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                        .withName("tls")
                        .withPort(9093)
                        .withType(KafkaListenerType.INTERNAL)
                        .withTls(true)
                        .withNewKafkaListenerAuthenticationOAuth()
                            .withClientId(OAUTH_KAFKA_BROKER_NAME)
                            .withNewClientSecret()
                                .withSecretName(OAUTH_KAFKA_BROKER_SECRET)
                                .withKey(OAUTH_KEY)
                            .endClientSecret()
                            .withAccessTokenIsJwt(false)
                            .withValidIssuerUri(keycloakInstance.getValidIssuerUri())
                            .withIntrospectionEndpointUri(keycloakInstance.getIntrospectionEndpointUri())
                            .withTlsTrustedCertificates(cert)
                            .withDisableTlsHostnameVerification(true)
                        .endKafkaListenerAuthenticationOAuth()
                        .build())
                .endKafka()
            .endSpec()
            .build());

        KafkaOauthClients oauthInternalClientIntrospectionJob = new KafkaOauthClientsBuilder()
                .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
                .withProducerName(producerName)
                .withConsumerName(consumerName)
                .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(introspectionKafka))
                .withTopicName(testStorage.getTopicName())
                .withMessageCount(testStorage.getMessageCount())
                .withOauthClientId(OAUTH_CLIENT_NAME)
                .withOauthClientSecret(OAUTH_CLIENT_SECRET)
                .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                .build();

        KubeResourceManager.get().createResourceWithWait(oauthInternalClientIntrospectionJob.producerStrimziOauthTls(introspectionKafka));
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, producerName, testStorage.getMessageCount());

        KubeResourceManager.get().createResourceWithWait(oauthInternalClientIntrospectionJob.consumerStrimziOauthTls(introspectionKafka));
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, consumerName, testStorage.getMessageCount());
    }

    @BeforeAll
    void setUp() {
        super.setupCoAndKeycloak(Environment.TEST_SUITE_NAMESPACE);

        keycloakInstance.setRealm("internal", true);

        LOGGER.info("Keycloak settings {}", keycloakInstance.toString());

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(Environment.TEST_SUITE_NAMESPACE, KafkaComponents.getBrokerPoolName(oauthClusterName), oauthClusterName, 3).build(),
            KafkaNodePoolTemplates.controllerPool(Environment.TEST_SUITE_NAMESPACE, KafkaComponents.getControllerPoolName(oauthClusterName), oauthClusterName, 3).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(Environment.TEST_SUITE_NAMESPACE, oauthClusterName, 3)
            .editSpec()
                .editKafka()
                    .withListeners(OauthAbstractST.BUILD_OAUTH_TLS_LISTENER.apply(keycloakInstance))
                .endKafka()
            .endSpec()
            .build());

        KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.tlsUser(Environment.TEST_SUITE_NAMESPACE, OAUTH_CLIENT_NAME, oauthClusterName).build());
    }
}



