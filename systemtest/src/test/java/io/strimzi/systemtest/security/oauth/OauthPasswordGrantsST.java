/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.oauth;

import io.strimzi.api.kafka.model.bridge.KafkaBridgeResources;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.mirrormaker.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpecBuilder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.FIPSNotSupported;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClients;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClientsBuilder;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaOauthClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaOauthClientsBuilder;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMakerTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.JobUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.WaitException;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.time.Duration;

import static io.strimzi.systemtest.TestConstants.ARM64_UNSUPPORTED;
import static io.strimzi.systemtest.TestConstants.BRIDGE;
import static io.strimzi.systemtest.TestConstants.CONNECT;
import static io.strimzi.systemtest.TestConstants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.TestConstants.HTTP_BRIDGE_DEFAULT_PORT;
import static io.strimzi.systemtest.TestConstants.MIRROR_MAKER;
import static io.strimzi.systemtest.TestConstants.MIRROR_MAKER2;
import static io.strimzi.systemtest.TestConstants.OAUTH;
import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

@Tag(OAUTH)
@Tag(REGRESSION)
@Tag(ARM64_UNSUPPORTED)
@FIPSNotSupported("Keycloak is not customized to run on FIPS env - https://github.com/strimzi/strimzi-kafka-operator/issues/8331")
public class OauthPasswordGrantsST extends OauthAbstractST {
    protected static final Logger LOGGER = LogManager.getLogger(OauthAuthorizationST.class);
    private final String oauthClusterName = "oauth-pass-grants-cluster-name";
    private static final String TEST_REALM = "internal";
    private static final String ALICE_SECRET = "alice-secret";
    private static final String ALICE_USERNAME = "alice";
    private static final String ALICE_PASSWORD_KEY = "password";

    @ParallelTest
    @Tag(MIRROR_MAKER)
    void testPasswordGrantsKafkaMirrorMaker() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        KafkaOauthClients oauthExampleClients = new KafkaOauthClientsBuilder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(oauthClusterName, testStorage.getTopicName(), Environment.TEST_SUITE_NAMESPACE).build());
        resourceManager.createResourceWithWait(oauthExampleClients.producerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(testStorage.getProducerName(), Environment.TEST_SUITE_NAMESPACE, testStorage.getMessageCount());

        resourceManager.createResourceWithWait(oauthExampleClients.consumerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(testStorage.getConsumerName(), Environment.TEST_SUITE_NAMESPACE, testStorage.getMessageCount());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetBrokerPoolName(), testStorage.getTargetClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetControllerPoolName(), testStorage.getTargetClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getTargetClusterName(), 1, 1)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
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
                            .build())
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResourceWithWait(KafkaMirrorMakerTemplates.kafkaMirrorMaker(oauthClusterName, oauthClusterName, testStorage.getTargetClusterName(),
                ClientUtils.generateRandomConsumerGroup(), 1, false)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .editSpec()
                .withNewConsumer()
                    .withBootstrapServers(KafkaResources.plainBootstrapAddress(oauthClusterName))
                    .withGroupId(ClientUtils.generateRandomConsumerGroup())
                    .addToConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                    .withNewKafkaClientAuthenticationOAuth()
                        .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                        .withClientId(OAUTH_MM_CLIENT_ID)
                        .withUsername(ALICE_USERNAME)
                        .withNewPasswordSecret()
                            .withSecretName(ALICE_SECRET)
                            .withPassword(ALICE_PASSWORD_KEY)
                        .endPasswordSecret()
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
                    .withBootstrapServers(KafkaResources.plainBootstrapAddress(testStorage.getTargetClusterName()))
                    .withNewKafkaClientAuthenticationOAuth()
                        .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                        .withClientId(OAUTH_MM_CLIENT_ID)
                        .withUsername(ALICE_USERNAME)
                        .withNewPasswordSecret()
                            .withSecretName(ALICE_SECRET)
                            .withPassword(ALICE_PASSWORD_KEY)
                        .endPasswordSecret()
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

        final String kafkaMirrorMakerPodName = kubeClient().listPods(Environment.TEST_SUITE_NAMESPACE, oauthClusterName, Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker.RESOURCE_KIND).get(0).getMetadata().getName();
        final String kafkaMirrorMakerLogs = KubeClusterResource.cmdKubeClient(Environment.TEST_SUITE_NAMESPACE).execInCurrentNamespace(Level.DEBUG, "logs", kafkaMirrorMakerPodName).out();
        verifyOauthConfiguration(kafkaMirrorMakerLogs);

        TestUtils.waitFor("MirrorMaker to copy messages from " + oauthClusterName + " to " + testStorage.getTargetClusterName(),
            TestConstants.GLOBAL_CLIENTS_POLL, TestConstants.TIMEOUT_FOR_MIRROR_MAKER_COPY_MESSAGES_BETWEEN_BROKERS,
            () -> {
                LOGGER.info("Deleting the Job");
                JobUtils.deleteJobWithWait(Environment.TEST_SUITE_NAMESPACE, OAUTH_CONSUMER_NAME);

                LOGGER.info("Creating new client with new consumer-group and also to point on {} cluster", testStorage.getTargetClusterName());
                KafkaOauthClients kafkaOauthClientJob = new KafkaOauthClientsBuilder()
                    .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
                    .withProducerName(testStorage.getConsumerName())
                    .withConsumerName(OAUTH_CONSUMER_NAME)
                    .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getTargetClusterName()))
                    .withTopicName(testStorage.getTopicName())
                    .withMessageCount(testStorage.getMessageCount())
                    .withOauthClientId(OAUTH_CLIENT_NAME)
                    .withOauthClientSecret(OAUTH_CLIENT_SECRET)
                    .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                    .build();

                resourceManager.createResourceWithWait(kafkaOauthClientJob.consumerStrimziOauthPlain());

                try {
                    ClientUtils.waitForClientSuccess(OAUTH_CONSUMER_NAME, Environment.TEST_SUITE_NAMESPACE, testStorage.getMessageCount());
                    return  true;
                } catch (WaitException e) {
                    e.printStackTrace();
                    return false;
                }
            });
    }

    @ParallelTest
    @Tag(MIRROR_MAKER2)
    void testPasswordGrantsKafkaMirrorMaker2() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        KafkaOauthClients oauthExampleClients = new KafkaOauthClientsBuilder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(oauthClusterName, testStorage.getTopicName(), Environment.TEST_SUITE_NAMESPACE).build());
        resourceManager.createResourceWithWait(oauthExampleClients.producerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(testStorage.getProducerName(), Environment.TEST_SUITE_NAMESPACE, testStorage.getMessageCount());

        resourceManager.createResourceWithWait(oauthExampleClients.consumerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(testStorage.getConsumerName(), Environment.TEST_SUITE_NAMESPACE, testStorage.getMessageCount());

        String kafkaSourceClusterName = oauthClusterName;

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetBrokerPoolName(), testStorage.getTargetClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetControllerPoolName(), testStorage.getTargetClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getTargetClusterName(), 1, 1)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
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
                .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                .withClientId(OAUTH_MM2_CLIENT_ID)
                .withUsername(ALICE_USERNAME)
                .withNewPasswordSecret()
                    .withSecretName(ALICE_SECRET)
                    .withPassword(ALICE_PASSWORD_KEY)
                .endPasswordSecret()
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
                .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                .withClientId(OAUTH_MM2_CLIENT_ID)
                .withUsername(ALICE_USERNAME)
                .withNewPasswordSecret()
                    .withSecretName(ALICE_SECRET)
                    .withPassword(ALICE_PASSWORD_KEY)
                .endPasswordSecret()
                .withNewClientSecret()
                    .withSecretName(MIRROR_MAKER_2_OAUTH_SECRET)
                    .withKey(OAUTH_KEY)
                .endClientSecret()
                .withConnectTimeoutSeconds(CONNECT_TIMEOUT_S)
                .withReadTimeoutSeconds(READ_TIMEOUT_S)
            .endKafkaClientAuthenticationOAuth()
            .build();

        resourceManager.createResourceWithWait(KafkaMirrorMaker2Templates.kafkaMirrorMaker2(oauthClusterName, testStorage.getTargetClusterName(), kafkaSourceClusterName, 1, false)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .editSpec()
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
            Duration.ofSeconds(30).toMillis(), TestConstants.TIMEOUT_FOR_MIRROR_MAKER_COPY_MESSAGES_BETWEEN_BROKERS,
            () -> {
                LOGGER.info("Deleting Job: {}/{}", Environment.TEST_SUITE_NAMESPACE, testStorage.getConsumerName());
                JobUtils.deleteJobWithWait(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName());

                LOGGER.info("Creating new client with new consumer group and also to point on {} cluster", testStorage.getTargetClusterName());

                KafkaOauthClients kafkaOauthClientJob = new KafkaOauthClientsBuilder()
                    .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
                    .withProducerName(testStorage.getProducerName())
                    .withConsumerName(testStorage.getConsumerName())
                    .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getTargetClusterName()))
                    .withTopicName(kafkaSourceClusterName + "." + testStorage.getTopicName())
                    .withMessageCount(testStorage.getMessageCount())
                    .withOauthClientId(OAUTH_CLIENT_NAME)
                    .withOauthClientSecret(OAUTH_CLIENT_SECRET)
                    .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                    .build();

                resourceManager.createResourceWithWait(kafkaOauthClientJob.consumerStrimziOauthPlain());

                try {
                    ClientUtils.waitForClientSuccess(testStorage.getConsumerName(), Environment.TEST_SUITE_NAMESPACE, testStorage.getMessageCount());
                    return  true;
                } catch (WaitException e) {
                    e.printStackTrace();
                    return false;
                }
            });
    }

    @ParallelTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testPasswordGrantsKafkaConnect() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        KafkaOauthClients oauthExampleClients = new KafkaOauthClientsBuilder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(oauthClusterName, testStorage.getTopicName(), Environment.TEST_SUITE_NAMESPACE).build());
        resourceManager.createResourceWithWait(oauthExampleClients.producerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(testStorage.getProducerName(), Environment.TEST_SUITE_NAMESPACE, testStorage.getMessageCount());

        resourceManager.createResourceWithWait(oauthExampleClients.consumerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(testStorage.getConsumerName(), Environment.TEST_SUITE_NAMESPACE, testStorage.getMessageCount());

        KafkaConnect connect = KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getClusterName(), Environment.TEST_SUITE_NAMESPACE, oauthClusterName, 1)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .editOrNewSpec()
                .withBootstrapServers(KafkaResources.plainBootstrapAddress(oauthClusterName))
                .withConfig(connectorConfig)
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .withNewKafkaClientAuthenticationOAuth()
                    .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                    .withClientId(OAUTH_CONNECT_CLIENT_ID)
                    .withUsername(ALICE_USERNAME)
                    .withNewPasswordSecret()
                        .withSecretName(ALICE_SECRET)
                        .withPassword(ALICE_PASSWORD_KEY)
                    .endPasswordSecret()
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
            .endSpec()
            .build();
        // This is required to be able to remove the TLS setting, the builder cannot remove it
        connect.getSpec().setTls(null);

        resourceManager.createResourceWithWait(connect);

        final String kafkaConnectPodName = kubeClient().listPods(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();

        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(Environment.TEST_SUITE_NAMESPACE, kafkaConnectPodName);

        KafkaConnectorUtils.createFileSinkConnector(Environment.TEST_SUITE_NAMESPACE, kafkaConnectPodName, testStorage.getTopicName(), TestConstants.DEFAULT_SINK_FILE_PATH, "http://localhost:8083");

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(Environment.TEST_SUITE_NAMESPACE, kafkaConnectPodName, TestConstants.DEFAULT_SINK_FILE_PATH, testStorage.getMessageCount());

        final String kafkaConnectLogs = KubeClusterResource.cmdKubeClient(Environment.TEST_SUITE_NAMESPACE).execInCurrentNamespace(Level.DEBUG, "logs", kafkaConnectPodName).out();
        verifyOauthConfiguration(kafkaConnectLogs);
    }

    @ParallelTest
    @Tag(BRIDGE)
    void testPasswordGrantsKafkaBridge() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(oauthClusterName, testStorage.getTopicName(), Environment.TEST_SUITE_NAMESPACE).build());

        KafkaOauthClients oauthExampleClients = new KafkaOauthClientsBuilder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResourceWithWait(oauthExampleClients.producerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(testStorage.getProducerName(), Environment.TEST_SUITE_NAMESPACE, testStorage.getMessageCount());

        resourceManager.createResourceWithWait(oauthExampleClients.consumerStrimziOauthPlain());
        ClientUtils.waitForClientSuccess(testStorage.getConsumerName(), Environment.TEST_SUITE_NAMESPACE, testStorage.getMessageCount());

        resourceManager.createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(oauthClusterName, KafkaResources.plainBootstrapAddress(oauthClusterName), 1)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .editSpec()
                .withNewKafkaClientAuthenticationOAuth()
                    .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                    .withClientId(OAUTH_BRIDGE_CLIENT_ID)
                    .withUsername(ALICE_USERNAME)
                    .withNewPasswordSecret()
                        .withSecretName(ALICE_SECRET)
                        .withPassword(ALICE_PASSWORD_KEY)
                    .endPasswordSecret()
                    .withNewClientSecret()
                        .withSecretName(BRIDGE_OAUTH_SECRET)
                        .withKey(OAUTH_KEY)
                    .endClientSecret()
                .endKafkaClientAuthenticationOAuth()
            .endSpec()
            .build());

        String producerName = "bridge-producer-" + testStorage.getClusterName();

        BridgeClients kafkaBridgeClientJob = new BridgeClientsBuilder()
            .withProducerName(producerName)
            .withBootstrapAddress(KafkaBridgeResources.serviceName(oauthClusterName))
            .withComponentName(KafkaBridgeResources.componentName(oauthClusterName))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withPort(HTTP_BRIDGE_DEFAULT_PORT)
            .withDelayMs(1000)
            .withPollInterval(1000)
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .build();

        resourceManager.createResourceWithWait(kafkaBridgeClientJob.producerStrimziBridge());
        ClientUtils.waitForClientSuccess(producerName, Environment.TEST_SUITE_NAMESPACE, testStorage.getMessageCount());
    }

    @BeforeAll
    void setUp()  {
        super.setupCoAndKeycloak(Environment.TEST_SUITE_NAMESPACE);

        keycloakInstance.setRealm(TEST_REALM, false);

        LOGGER.info("Keycloak settings {}", keycloakInstance.toString());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(Environment.TEST_SUITE_NAMESPACE, KafkaNodePoolResource.getBrokerPoolName(oauthClusterName), oauthClusterName, 3).build(),
                KafkaNodePoolTemplates.controllerPool(Environment.TEST_SUITE_NAMESPACE, KafkaNodePoolResource.getControllerPoolName(oauthClusterName), oauthClusterName, 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(oauthClusterName, 3)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
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
                        .endKafkaListenerAuthenticationOAuth()
                        .build())
                .endKafka()
            .endSpec()
            .build());

        SecretUtils.createSecret(Environment.TEST_SUITE_NAMESPACE, ALICE_SECRET, ALICE_PASSWORD_KEY, "alice-password");
    }
}
