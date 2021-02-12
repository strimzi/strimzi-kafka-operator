/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.oauth;

import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2ClusterSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2ClusterSpecBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBridgeExampleClients;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaOauthExampleClients;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMakerTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.JobUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.WaitException;
import io.vertx.core.cli.annotations.Description;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.time.Duration;

import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.HTTP_BRIDGE_DEFAULT_PORT;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER2;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.OAUTH;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@Tag(OAUTH)
@Tag(REGRESSION)
public class OauthPlainST extends OauthAbstractST {
    protected static final Logger LOGGER = LogManager.getLogger(OauthPlainST.class);

    private KafkaOauthExampleClients oauthInternalClientJob;
    private KafkaOauthExampleClients oauthInternalClientChecksJob;
    private final String oauthClusterName = "oauth-cluster-plain-name";
    private final String audienceListenerPort = "9098";
    private final String customClaimListenerPort = "9099";
    private static final String NAMESPACE = "oauth2-plain-cluster-test";

    @Description(
            "As an oauth producer, I should be able to produce messages to the kafka broker\n" +
            "As an oauth consumer, I should be able to consumer messages from the kafka broker.")
    @ParallelTest
    void testProducerConsumer(ExtensionContext extensionContext) {
        String clusterName = mapTestWithClusterNames.get(extensionContext.getDisplayName());
        String producerName = OAUTH_PRODUCER_NAME + "-" + clusterName;
        String consumerName = OAUTH_CONSUMER_NAME + "-" + clusterName;
        String topicName = mapTestWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, topicName).build());

        KafkaOauthExampleClients oauthExampleClients = new KafkaOauthExampleClients.Builder()
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withOAuthClientId(OAUTH_CLIENT_NAME)
            .withOAuthClientSecret(OAUTH_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResource(extensionContext, oauthExampleClients.producerStrimziOauthPlain().build());
        ClientUtils.waitForClientSuccess(producerName, NAMESPACE, MESSAGE_COUNT);

        resourceManager.createResource(extensionContext, oauthExampleClients.consumerStrimziOauthPlain().build());
        ClientUtils.waitForClientSuccess(consumerName, NAMESPACE, MESSAGE_COUNT);
    }

    @Test
    void testSaslPlainProducerConsumer() {
        String plainAdditionalConfig =
                "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=%s password=%s;\n" +
                        "sasl.mechanism=PLAIN";

        KafkaOauthExampleClients plainSaslOauthConsumerClientsJob = new KafkaOauthExampleClients.Builder()
                .withConsumerName(OAUTH_CLIENT_AUDIENCE_CONSUMER)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
                .withTopicName(TOPIC_NAME)
                .withMessageCount(MESSAGE_COUNT)
                .withOAuthClientId(OAUTH_CLIENT_NAME)
                .withOAuthClientSecret(OAUTH_CLIENT_SECRET)
                .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                .withAdditionalConfig(String.format(plainAdditionalConfig, OAUTH_CLIENT_AUDIENCE_CONSUMER, OAUTH_CLIENT_AUDIENCE_SECRET))
                .build();

        KafkaOauthExampleClients plainSaslOauthProducerClientsJob = new KafkaOauthExampleClients.Builder()
                .withProducerName(OAUTH_CLIENT_AUDIENCE_PRODUCER)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
                .withTopicName(TOPIC_NAME)
                .withMessageCount(MESSAGE_COUNT)
                .withOAuthClientId(OAUTH_CLIENT_NAME)
                .withOAuthClientSecret(OAUTH_CLIENT_SECRET)
                .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                .withAdditionalConfig(String.format(plainAdditionalConfig, OAUTH_CLIENT_AUDIENCE_PRODUCER, OAUTH_CLIENT_AUDIENCE_SECRET))
                .build();

        plainSaslOauthProducerClientsJob.createAndWaitForReadiness(plainSaslOauthProducerClientsJob.producerStrimziOauthPlain().build());
        ClientUtils.waitForClientSuccess(OAUTH_CLIENT_AUDIENCE_PRODUCER, NAMESPACE, MESSAGE_COUNT);

        plainSaslOauthConsumerClientsJob.createAndWaitForReadiness(plainSaslOauthConsumerClientsJob.consumerStrimziOauthPlain().build());
        ClientUtils.waitForClientSuccess(OAUTH_CLIENT_AUDIENCE_CONSUMER, NAMESPACE, MESSAGE_COUNT);
    }

    @Test
    void testProducerConsumerAudienceTokenChecks() {
        LOGGER.info("Setting producer and consumer properties");
        oauthInternalClientJob = oauthInternalClientJob.toBuilder()
                .withBootstrapAddress(KafkaResources.bootstrapServiceName(oauthClusterName) + ":" + audienceListenerPort)
                .build();

        LOGGER.info("Use clients without access token containing audience token");
        oauthInternalClientJob.createAndWaitForReadiness(oauthInternalClientJob.producerStrimziOauthPlain().build());
        assertDoesNotThrow(() -> ClientUtils.waitForClientTimeout(OAUTH_PRODUCER_NAME, NAMESPACE, MESSAGE_COUNT));
        oauthInternalClientJob.createAndWaitForReadiness(oauthInternalClientJob.consumerStrimziOauthPlain().build());
        assertDoesNotThrow(() -> ClientUtils.waitForClientTimeout(OAUTH_CONSUMER_NAME, NAMESPACE, MESSAGE_COUNT));

        JobUtils.deleteJobWithWait(NAMESPACE, OAUTH_PRODUCER_NAME);
        JobUtils.deleteJobWithWait(NAMESPACE, OAUTH_CONSUMER_NAME);

        LOGGER.info("Use clients with Access token containing audience token");
        oauthInternalClientChecksJob.createAndWaitForReadiness(oauthInternalClientChecksJob.producerStrimziOauthPlain().build());
        ClientUtils.waitForClientSuccess(OAUTH_CLIENT_AUDIENCE_PRODUCER, NAMESPACE, MESSAGE_COUNT);

        oauthInternalClientChecksJob.createAndWaitForReadiness(oauthInternalClientChecksJob.consumerStrimziOauthPlain().build());
        ClientUtils.waitForClientSuccess(OAUTH_CLIENT_AUDIENCE_CONSUMER, NAMESPACE, MESSAGE_COUNT);

        JobUtils.deleteJobWithWait(NAMESPACE, OAUTH_CLIENT_AUDIENCE_PRODUCER);
        JobUtils.deleteJobWithWait(NAMESPACE, OAUTH_CLIENT_AUDIENCE_CONSUMER);

        oauthInternalClientJob = oauthInternalClientJob.toBuilder()
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
                .build();
    }

    @Test
    void testAccessTokenClaimCheck() {
        LOGGER.info("Use clients with clientId not containing 'hello-world' in access token.");
        oauthInternalClientChecksJob = oauthInternalClientChecksJob.toBuilder()
                .withBootstrapAddress(KafkaResources.bootstrapServiceName(oauthClusterName) + ":" + customClaimListenerPort)
                .build();

        oauthInternalClientChecksJob.createAndWaitForReadiness(oauthInternalClientChecksJob.producerStrimziOauthPlain().build());
        assertDoesNotThrow(() -> ClientUtils.waitForClientTimeout(OAUTH_CLIENT_AUDIENCE_PRODUCER, NAMESPACE, MESSAGE_COUNT));

        oauthInternalClientChecksJob.createAndWaitForReadiness(oauthInternalClientChecksJob.consumerStrimziOauthPlain().build());
        assertDoesNotThrow(() -> ClientUtils.waitForClientTimeout(OAUTH_CLIENT_AUDIENCE_CONSUMER, NAMESPACE, MESSAGE_COUNT));

        JobUtils.deleteJobWithWait(NAMESPACE, OAUTH_CLIENT_AUDIENCE_PRODUCER);
        JobUtils.deleteJobWithWait(NAMESPACE, OAUTH_CLIENT_AUDIENCE_CONSUMER);

        LOGGER.info("Use clients with clientId containing 'hello-world' in access token.");
        oauthInternalClientJob = oauthInternalClientJob.toBuilder().withBootstrapAddress(
                KafkaResources.bootstrapServiceName(oauthClusterName) + ":" + customClaimListenerPort).build();

        oauthInternalClientJob.createAndWaitForReadiness(oauthInternalClientJob.producerStrimziOauthPlain().build());
        ClientUtils.waitForClientSuccess(OAUTH_PRODUCER_NAME, NAMESPACE, MESSAGE_COUNT);
        oauthInternalClientJob.createAndWaitForReadiness(oauthInternalClientJob.consumerStrimziOauthPlain().build());
        ClientUtils.waitForClientSuccess(OAUTH_CONSUMER_NAME, NAMESPACE, MESSAGE_COUNT);

        JobUtils.deleteJobWithWait(NAMESPACE, OAUTH_PRODUCER_NAME);
        JobUtils.deleteJobWithWait(NAMESPACE, OAUTH_CONSUMER_NAME);

        oauthInternalClientJob = oauthInternalClientJob.toBuilder()
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
                .build();
    }

    @Description("As an oauth KafkaConnect, I should be able to sink messages from kafka broker topic.")
    @ParallelTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testProducerConsumerConnect(ExtensionContext extensionContext) {
        String kafkaClientsName = mapTestWithKafkaClientNames.get(extensionContext.getDisplayName());
        String clusterName = mapTestWithClusterNames.get(extensionContext.getDisplayName());
        String producerName = OAUTH_PRODUCER_NAME + "-" + clusterName;
        String consumerName = OAUTH_CONSUMER_NAME + "-" + clusterName;
        String topicName = mapTestWithTestTopics.get(extensionContext.getDisplayName());

        KafkaOauthExampleClients oauthExampleClients = new KafkaOauthExampleClients.Builder()
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withOAuthClientId(OAUTH_CLIENT_NAME)
            .withOAuthClientSecret(OAUTH_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, topicName).build());
        resourceManager.createResource(extensionContext, oauthExampleClients.producerStrimziOauthPlain().build());
        ClientUtils.waitForClientSuccess(producerName, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, producerName);

        resourceManager.createResource(extensionContext, oauthExampleClients.consumerStrimziOauthPlain().build());
        ClientUtils.waitForClientSuccess(consumerName, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, consumerName);

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, kafkaClientsName).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, oauthClusterName, 1)
            .withNewSpec()
                .withReplicas(1)
                .withBootstrapServers(KafkaResources.plainBootstrapAddress(oauthClusterName))
                .withConfig(connectorConfig)
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .withNewKafkaClientAuthenticationOAuth()
                    .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                    .withClientId("kafka-connect")
                    .withNewClientSecret()
                        .withSecretName(CONNECT_OAUTH_SECRET)
                        .withKey(OAUTH_KEY)
                    .endClientSecret()
                .endKafkaClientAuthenticationOAuth()
                .withTls(null)
            .endSpec()
            .build());

        String kafkaConnectPodName = kubeClient().listPods(clusterName, Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();

        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(kafkaConnectPodName);

        KafkaConnectorUtils.createFileSinkConnector(kafkaConnectPodName, topicName, Constants.DEFAULT_SINK_FILE_PATH, "http://localhost:8083");

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(kafkaConnectPodName, Constants.DEFAULT_SINK_FILE_PATH);
    }

    @Description("As an oauth mirror maker, I should be able to replicate topic data between kafka clusters")
    @ParallelTest
    @Tag(MIRROR_MAKER)
    @Tag(NODEPORT_SUPPORTED)
    void testProducerConsumerMirrorMaker(ExtensionContext extensionContext) {
        String clusterName = mapTestWithClusterNames.get(extensionContext.getDisplayName());
        String producerName = OAUTH_PRODUCER_NAME + "-" + clusterName;
        String consumerName = OAUTH_CONSUMER_NAME + "-" + clusterName;
        String topicName = mapTestWithTestTopics.get(extensionContext.getDisplayName());

        KafkaOauthExampleClients oauthExampleClients = new KafkaOauthExampleClients.Builder()
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withOAuthClientId(OAUTH_CLIENT_NAME)
            .withOAuthClientSecret(OAUTH_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, topicName).build());
        resourceManager.createResource(extensionContext, oauthExampleClients.producerStrimziOauthPlain().build());
        ClientUtils.waitForClientSuccess(producerName, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, producerName);

        resourceManager.createResource(extensionContext, oauthExampleClients.consumerStrimziOauthPlain().build());
        ClientUtils.waitForClientSuccess(consumerName, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, consumerName);

        String targetKafkaCluster = clusterName + "-target";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(targetKafkaCluster, 1, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
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
                        .endGenericKafkaListener()
                        .addNewGenericKafkaListener()
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
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(oauthClusterName, oauthClusterName, targetKafkaCluster,
            ClientUtils.generateRandomConsumerGroup(), 1, false)
                .editSpec()
                    .withNewConsumer()
                        .withBootstrapServers(KafkaResources.plainBootstrapAddress(oauthClusterName))
                        .withGroupId(ClientUtils.generateRandomConsumerGroup())
                        .addToConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                        .withNewKafkaClientAuthenticationOAuth()
                            .withNewTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                            .withClientId("kafka-mirror-maker")
                            .withNewClientSecret()
                                .withSecretName(MIRROR_MAKER_OAUTH_SECRET)
                                .withKey(OAUTH_KEY)
                            .endClientSecret()
                        .endKafkaClientAuthenticationOAuth()
                        .withTls(null)
                    .endConsumer()
                    .withNewProducer()
                        .withBootstrapServers(KafkaResources.plainBootstrapAddress(targetKafkaCluster))
                        .withNewKafkaClientAuthenticationOAuth()
                            .withNewTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                            .withClientId("kafka-mirror-maker")
                            .withNewClientSecret()
                                .withSecretName(MIRROR_MAKER_OAUTH_SECRET)
                                .withKey(OAUTH_KEY)
                            .endClientSecret()
                        .endKafkaClientAuthenticationOAuth()
                        .addToConfig(ProducerConfig.ACKS_CONFIG, "all")
                        .withTls(null)
                    .endProducer()
                .endSpec()
                .build());

        TestUtils.waitFor("Waiting for Mirror Maker will copy messages from " + oauthClusterName + " to " + targetKafkaCluster,
            Constants.GLOBAL_CLIENTS_POLL, Constants.TIMEOUT_FOR_MIRROR_MAKER_COPY_MESSAGES_BETWEEN_BROKERS,
            () -> {
                LOGGER.info("Deleting the Job");
                JobUtils.deleteJobWithWait(NAMESPACE, OAUTH_CONSUMER_NAME);

                LOGGER.info("Creating new client with new consumer-group and also to point on {} cluster", targetKafkaCluster);
                KafkaOauthExampleClients kafkaOauthClientJob = new KafkaOauthExampleClients.Builder()
                    .withProducerName(consumerName)
                    .withConsumerName(OAUTH_CONSUMER_NAME)
                    .withBootstrapAddress(KafkaResources.plainBootstrapAddress(targetKafkaCluster))
                    .withTopicName(topicName)
                    .withMessageCount(MESSAGE_COUNT)
                    .withOAuthClientId(OAUTH_CLIENT_NAME)
                    .withOAuthClientSecret(OAUTH_CLIENT_SECRET)
                    .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                    .build();

                resourceManager.createResource(extensionContext, kafkaOauthClientJob.consumerStrimziOauthPlain().build());

                try {
                    ClientUtils.waitForClientSuccess(OAUTH_CONSUMER_NAME, NAMESPACE, MESSAGE_COUNT);
                    return  true;
                } catch (WaitException e) {
                    e.printStackTrace();
                    return false;
                }
            });
    }

    @ParallelTest
    @Tag(MIRROR_MAKER2)
    @Tag(CONNECT_COMPONENTS)
    @Tag(NODEPORT_SUPPORTED)
    void testProducerConsumerMirrorMaker2(ExtensionContext extensionContext) {
        String clusterName = mapTestWithClusterNames.get(extensionContext.getDisplayName());
        String producerName = OAUTH_PRODUCER_NAME + "-" + clusterName;
        String consumerName = OAUTH_CONSUMER_NAME + "-" + clusterName;
        String topicName = mapTestWithTestTopics.get(extensionContext.getDisplayName());

        KafkaOauthExampleClients oauthExampleClients = new KafkaOauthExampleClients.Builder()
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withOAuthClientId(OAUTH_CLIENT_NAME)
            .withOAuthClientSecret(OAUTH_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, topicName).build());
        resourceManager.createResource(extensionContext, oauthExampleClients.producerStrimziOauthPlain().build());
        ClientUtils.waitForClientSuccess(producerName, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, producerName);

        resourceManager.createResource(extensionContext, oauthExampleClients.consumerStrimziOauthPlain().build());
        ClientUtils.waitForClientSuccess(consumerName, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, consumerName);

        String kafkaSourceClusterName = clusterName;
        String kafkaTargetClusterName = clusterName + "-target";
        // mirror maker 2 adding prefix to mirrored topic for in this case mirrotopic will be : my-cluster.my-topic
        String kafkaTargetClusterTopicName = kafkaSourceClusterName + "." + topicName;

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaTargetClusterName, 1, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
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
                        .endGenericKafkaListener()
                        .addNewGenericKafkaListener()
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
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        // Deploy Mirror Maker 2.0 with oauth
        KafkaMirrorMaker2ClusterSpec sourceClusterWithOauth = new KafkaMirrorMaker2ClusterSpecBuilder()
            .withAlias(kafkaSourceClusterName)
            .withConfig(connectorConfig)
            .withBootstrapServers(KafkaResources.plainBootstrapAddress(kafkaSourceClusterName))
            .withNewKafkaClientAuthenticationOAuth()
                .withNewTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                .withClientId("kafka-mirror-maker-2")
                .withNewClientSecret()
                    .withSecretName(MIRROR_MAKER_2_OAUTH_SECRET)
                    .withKey(OAUTH_KEY)
                .endClientSecret()
            .endKafkaClientAuthenticationOAuth()
            .build();

        KafkaMirrorMaker2ClusterSpec targetClusterWithOauth = new KafkaMirrorMaker2ClusterSpecBuilder()
            .withAlias(kafkaTargetClusterName)
            .withConfig(connectorConfig)
            .withBootstrapServers(KafkaResources.plainBootstrapAddress(kafkaTargetClusterName))
            .withNewKafkaClientAuthenticationOAuth()
                .withNewTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                .withClientId("kafka-mirror-maker-2")
                .withNewClientSecret()
                    .withSecretName(MIRROR_MAKER_2_OAUTH_SECRET)
                    .withKey(OAUTH_KEY)
                .endClientSecret()
            .endKafkaClientAuthenticationOAuth()
            .build();

        resourceManager.createResource(extensionContext, KafkaMirrorMaker2Templates.kafkaMirrorMaker2(oauthClusterName, kafkaTargetClusterName, kafkaSourceClusterName, 1, false)
            .editSpec()
                .withClusters(sourceClusterWithOauth, targetClusterWithOauth)
                .editFirstMirror()
                    .withNewSourceCluster(kafkaSourceClusterName)
                .endMirror()
            .endSpec()
            .build());

        TestUtils.waitFor("Waiting for Mirror Maker 2 will copy messages from " + kafkaSourceClusterName + " to " + kafkaTargetClusterName,
            Duration.ofSeconds(30).toMillis(), Constants.TIMEOUT_FOR_MIRROR_MAKER_COPY_MESSAGES_BETWEEN_BROKERS,
            () -> {
                LOGGER.info("Deleting the Job {}", consumerName);
                JobUtils.deleteJobWithWait(NAMESPACE, consumerName);

                LOGGER.info("Creating new client with new consumer-group and also to point on {} cluster", kafkaTargetClusterName);

                KafkaOauthExampleClients kafkaOauthClientJob = new KafkaOauthExampleClients.Builder()
                    .withProducerName(producerName)
                    .withConsumerName(consumerName)
                    .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaTargetClusterName))
                    .withTopicName(kafkaTargetClusterTopicName)
                    .withMessageCount(MESSAGE_COUNT)
                    .withOAuthClientId(OAUTH_CLIENT_NAME)
                    .withOAuthClientSecret(OAUTH_CLIENT_SECRET)
                    .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                    .build();

                resourceManager.createResource(extensionContext, kafkaOauthClientJob.consumerStrimziOauthPlain().build());

                try {
                    ClientUtils.waitForClientSuccess(consumerName, NAMESPACE, MESSAGE_COUNT);
                    return  true;
                } catch (WaitException e) {
                    e.printStackTrace();
                    return false;
                }
            });
    }

    @Description("As a oauth bridge, I should be able to send messages to bridge endpoint.")
    @ParallelTest
    @Tag(BRIDGE)
    void testProducerConsumerBridge(ExtensionContext extensionContext) {
        String kafkaClientsName = mapTestWithKafkaClientNames.get(extensionContext.getDisplayName());
        String clusterName = mapTestWithClusterNames.get(extensionContext.getDisplayName());
        String producerName = OAUTH_PRODUCER_NAME + "-" + clusterName;
        String consumerName = OAUTH_CONSUMER_NAME + "-" + clusterName;
        String topicName = mapTestWithTestTopics.get(extensionContext.getDisplayName());

        KafkaOauthExampleClients oauthExampleClients = new KafkaOauthExampleClients.Builder()
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withOAuthClientId(OAUTH_CLIENT_NAME)
            .withOAuthClientSecret(OAUTH_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, topicName).build());
        resourceManager.createResource(extensionContext, oauthExampleClients.producerStrimziOauthPlain().build());
        ClientUtils.waitForClientSuccess(producerName, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, producerName);

        resourceManager.createResource(extensionContext, oauthExampleClients.consumerStrimziOauthPlain().build());
        ClientUtils.waitForClientSuccess(consumerName, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, consumerName);

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, kafkaClientsName).build());
        resourceManager.createResource(extensionContext, KafkaBridgeTemplates.kafkaBridge(oauthClusterName, KafkaResources.plainBootstrapAddress(oauthClusterName), 1)
            .editSpec()
                .withNewKafkaClientAuthenticationOAuth()
                    .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                    .withClientId("kafka-bridge")
                    .withNewClientSecret()
                        .withSecretName(BRIDGE_OAUTH_SECRET)
                        .withKey(OAUTH_KEY)
                    .endClientSecret()
                .endKafkaClientAuthenticationOAuth()
            .endSpec()
            .build());

        String bridgeProducerName = "bridge-producer-" + clusterName;

        KafkaBridgeExampleClients kafkaBridgeClientJob = new KafkaBridgeExampleClients.Builder()
            .withProducerName(bridgeProducerName)
            .withBootstrapAddress(KafkaBridgeResources.serviceName(oauthClusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withPort(HTTP_BRIDGE_DEFAULT_PORT)
            .withDelayMs(1000)
            .withPollInterval(1000)
            .build();

        resourceManager.createResource(extensionContext, kafkaBridgeClientJob.producerStrimziBridge().build());
        ClientUtils.waitForClientSuccess(bridgeProducerName, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, bridgeProducerName);
    }

    @BeforeAll
    void setUp(ExtensionContext extensionContext) {
        super.beforeAllMayOverride(extensionContext);
        // for namespace
        super.beforeAllOverrideMe(extensionContext, NAMESPACE);

        final String customClaimListener = "cclistener";
        final String audienceListener = "audlistnr";

        setupCoAndKeycloak(extensionContext);
        keycloakInstance.setRealm("internal", false);

        LOGGER.info("Setting producer and consumer properties");

        oauthInternalClientJob = new KafkaOauthExampleClients.Builder()
            .withProducerName(OAUTH_PRODUCER_NAME)
            .withConsumerName(OAUTH_CONSUMER_NAME)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
            .withTopicName(TOPIC_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withOAuthClientId(OAUTH_CLIENT_NAME)
            .withOAuthClientSecret(OAUTH_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        oauthInternalClientChecksJob = new KafkaOauthExampleClients.Builder()
                .withProducerName(OAUTH_CLIENT_AUDIENCE_PRODUCER)
                .withConsumerName(OAUTH_CLIENT_AUDIENCE_CONSUMER)
                .withBootstrapAddress(KafkaResources.bootstrapServiceName(oauthClusterName) + ":" + audienceListenerPort)
                .withTopicName(TOPIC_NAME)
                .withMessageCount(MESSAGE_COUNT)
                .withOAuthProducerClientId(OAUTH_CLIENT_AUDIENCE_PRODUCER)
                .withOAuthConsumerClientId(OAUTH_CLIENT_AUDIENCE_CONSUMER)
                .withOAuthClientSecret(OAUTH_CLIENT_AUDIENCE_SECRET)
                .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                .build();

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(oauthClusterName, 1, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
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
                            .endKafkaListenerAuthenticationOAuth()
                        .endGenericKafkaListener()
                        .addNewGenericKafkaListener()
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
                        .endGenericKafkaListener()
                        .addNewGenericKafkaListener()
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
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());
    }
}
