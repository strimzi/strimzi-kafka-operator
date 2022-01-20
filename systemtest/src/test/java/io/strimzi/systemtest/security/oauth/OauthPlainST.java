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
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.ParallelSuite;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.keycloak.KeycloakInstance;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBridgeExampleClients;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaOauthExampleClients;
import io.strimzi.systemtest.storage.TestStorage;
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
import io.strimzi.systemtest.utils.specific.KeycloakUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.WaitException;
import io.vertx.core.cli.annotations.Description;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
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
@ParallelSuite
public class OauthPlainST extends OauthAbstractST {
    protected static final Logger LOGGER = LogManager.getLogger(OauthPlainST.class);

    private KeycloakInstance keycloakInstance;
    private final String oauthClusterName = "oauth-cluster-plain-name";
    private final String customClaimListenerPort = "9099";
    private final String namespace = testSuiteNamespaceManager.getMapOfAdditionalNamespaces().get(OauthPlainST.class.getSimpleName()).stream().findFirst().get();

    @Description(
            "As an oauth producer, I should be able to produce messages to the kafka broker\n" +
            "As an oauth consumer, I should be able to consumer messages from the kafka broker.")
    @ParallelTest
    void testProducerConsumer(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String producerName = OAUTH_PRODUCER_NAME + "-" + clusterName;
        String consumerName = OAUTH_CONSUMER_NAME + "-" + clusterName;
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, topicName, namespace).build());

        KafkaOauthExampleClients oauthExampleClients = new KafkaOauthExampleClients.Builder()
            .withNamespaceName(namespace)
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
        ClientUtils.waitForClientSuccess(producerName, namespace, MESSAGE_COUNT);

        resourceManager.createResource(extensionContext, oauthExampleClients.consumerStrimziOauthPlain().build());
        ClientUtils.waitForClientSuccess(consumerName, namespace, MESSAGE_COUNT);
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

        KafkaOauthExampleClients plainSaslOauthConsumerClientsJob = new KafkaOauthExampleClients.Builder()
            .withNamespaceName(namespace)
            .withConsumerName(audienceConsumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withOAuthClientId(OAUTH_CLIENT_NAME)
            .withOAuthClientSecret(OAUTH_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .withAdditionalConfig(String.format(plainAdditionalConfig, OAUTH_CLIENT_AUDIENCE_CONSUMER, OAUTH_CLIENT_AUDIENCE_SECRET))
            .build();

        KafkaOauthExampleClients plainSaslOauthProducerClientsJob = new KafkaOauthExampleClients.Builder()
            .withNamespaceName(namespace)
            .withProducerName(audienceProducerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withOAuthClientId(OAUTH_CLIENT_NAME)
            .withOAuthClientSecret(OAUTH_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .withAdditionalConfig(String.format(plainAdditionalConfig, OAUTH_CLIENT_AUDIENCE_PRODUCER, OAUTH_CLIENT_AUDIENCE_SECRET))
            .build();

        resourceManager.createResource(extensionContext, plainSaslOauthProducerClientsJob.producerStrimziOauthPlain().build());
        ClientUtils.waitForClientSuccess(audienceProducerName, namespace, MESSAGE_COUNT);

        resourceManager.createResource(extensionContext, plainSaslOauthConsumerClientsJob.consumerStrimziOauthPlain().build());
        ClientUtils.waitForClientSuccess(audienceConsumerName, namespace, MESSAGE_COUNT);
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
        KafkaOauthExampleClients oauthInternalClientJob = new KafkaOauthExampleClients.Builder()
            .withNamespaceName(namespace)
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.bootstrapServiceName(oauthClusterName) + ":" + audienceListenerPort)
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withOAuthClientId(OAUTH_CLIENT_NAME)
            .withOAuthClientSecret(OAUTH_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        LOGGER.info("Use clients without access token containing audience token");
        resourceManager.createResource(extensionContext, oauthInternalClientJob.producerStrimziOauthPlain().build());
        assertDoesNotThrow(() -> ClientUtils.waitForClientTimeout(producerName, namespace, MESSAGE_COUNT));
        resourceManager.createResource(extensionContext, oauthInternalClientJob.consumerStrimziOauthPlain().build());
        assertDoesNotThrow(() -> ClientUtils.waitForClientTimeout(consumerName, namespace, MESSAGE_COUNT));

        JobUtils.deleteJobWithWait(namespace, producerName);
        JobUtils.deleteJobWithWait(namespace, consumerName);

        LOGGER.info("Use clients with Access token containing audience token");

        KafkaOauthExampleClients oauthAudienceInternalClientJob = new KafkaOauthExampleClients.Builder()
            .withNamespaceName(namespace)
            .withProducerName(audienceProducerName)
            .withConsumerName(audienceConsumerName)
            .withBootstrapAddress(KafkaResources.bootstrapServiceName(oauthClusterName) + ":" + customClaimListenerPort)
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withOAuthClientId(OAUTH_CLIENT_NAME)
            .withOAuthClientSecret(OAUTH_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResource(extensionContext, oauthAudienceInternalClientJob.producerStrimziOauthPlain().build());
        ClientUtils.waitForClientSuccess(audienceProducerName, namespace, MESSAGE_COUNT);

        resourceManager.createResource(extensionContext, oauthAudienceInternalClientJob.consumerStrimziOauthPlain().build());
        ClientUtils.waitForClientSuccess(audienceConsumerName, namespace, MESSAGE_COUNT);

        JobUtils.deleteJobWithWait(namespace, audienceProducerName);
        JobUtils.deleteJobWithWait(namespace, audienceConsumerName);
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

        KafkaOauthExampleClients oauthAudienceInternalClientJob = new KafkaOauthExampleClients.Builder()
            .withNamespaceName(namespace)
            .withProducerName(audienceProducerName)
            .withConsumerName(audienceConsumerName)
            .withBootstrapAddress(KafkaResources.bootstrapServiceName(oauthClusterName) + ":" + customClaimListenerPort)
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withOAuthProducerClientId(OAUTH_CLIENT_AUDIENCE_PRODUCER)
            .withOAuthConsumerClientId(OAUTH_CLIENT_AUDIENCE_CONSUMER)
            .withOAuthClientSecret(OAUTH_CLIENT_AUDIENCE_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResource(extensionContext, oauthAudienceInternalClientJob.producerStrimziOauthPlain().build());
        assertDoesNotThrow(() -> ClientUtils.waitForClientTimeout(audienceProducerName, namespace, MESSAGE_COUNT));
        resourceManager.createResource(extensionContext, oauthAudienceInternalClientJob.consumerStrimziOauthPlain().build());
        assertDoesNotThrow(() -> ClientUtils.waitForClientTimeout(audienceConsumerName, namespace, MESSAGE_COUNT));

        JobUtils.deleteJobWithWait(namespace, audienceProducerName);
        JobUtils.deleteJobWithWait(namespace, audienceConsumerName);

        LOGGER.info("Use clients with clientId containing 'hello-world' in access token.");

        KafkaOauthExampleClients oauthInternalClientJob = new KafkaOauthExampleClients.Builder()
            .withNamespaceName(namespace)
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.bootstrapServiceName(oauthClusterName) + ":" + customClaimListenerPort)
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withOAuthClientId(OAUTH_CLIENT_NAME)
            .withOAuthClientSecret(OAUTH_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResource(extensionContext, oauthInternalClientJob.producerStrimziOauthPlain().build());
        ClientUtils.waitForClientSuccess(producerName, namespace, MESSAGE_COUNT);
        resourceManager.createResource(extensionContext, oauthInternalClientJob.consumerStrimziOauthPlain().build());
        ClientUtils.waitForClientSuccess(consumerName, namespace, MESSAGE_COUNT);

        JobUtils.deleteJobWithWait(namespace, producerName);
        JobUtils.deleteJobWithWait(namespace, consumerName);
    }

    @Description("As an oauth KafkaConnect, I should be able to sink messages from kafka broker topic.")
    @ParallelTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testProducerConsumerConnect(ExtensionContext extensionContext) {
        String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String producerName = OAUTH_PRODUCER_NAME + "-" + clusterName;
        String consumerName = OAUTH_CONSUMER_NAME + "-" + clusterName;
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        KafkaOauthExampleClients oauthExampleClients = new KafkaOauthExampleClients.Builder()
            .withNamespaceName(namespace)
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withOAuthClientId(OAUTH_CLIENT_NAME)
            .withOAuthClientSecret(OAUTH_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, topicName, namespace).build());
        resourceManager.createResource(extensionContext, oauthExampleClients.producerStrimziOauthPlain().build());
        ClientUtils.waitForClientSuccess(producerName, namespace, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(namespace, producerName);

        resourceManager.createResource(extensionContext, oauthExampleClients.consumerStrimziOauthPlain().build());
        ClientUtils.waitForClientSuccess(consumerName, namespace, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(namespace, consumerName);

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(namespace, false, kafkaClientsName).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, namespace, oauthClusterName, 1)
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

        String kafkaConnectPodName = kubeClient(namespace).listPods(namespace, clusterName, Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();

        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(namespace, kafkaConnectPodName);

        KafkaConnectorUtils.createFileSinkConnector(namespace, kafkaConnectPodName, topicName, Constants.DEFAULT_SINK_FILE_PATH, "http://localhost:8083");

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(namespace, kafkaConnectPodName, Constants.DEFAULT_SINK_FILE_PATH, "\"Hello-world - 99\"");
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

        KafkaOauthExampleClients oauthExampleClients = new KafkaOauthExampleClients.Builder()
            .withNamespaceName(namespace)
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withOAuthClientId(OAUTH_CLIENT_NAME)
            .withOAuthClientSecret(OAUTH_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, topicName, namespace).build());
        resourceManager.createResource(extensionContext, oauthExampleClients.producerStrimziOauthPlain().build());
        ClientUtils.waitForClientSuccess(producerName, namespace, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(namespace, producerName);

        resourceManager.createResource(extensionContext, oauthExampleClients.consumerStrimziOauthPlain().build());
        ClientUtils.waitForClientSuccess(consumerName, namespace, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(namespace, consumerName);

        String targetKafkaCluster = clusterName + "-target";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(targetKafkaCluster, 1, 1)
            .editMetadata()
                .withNamespace(namespace)
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
                    .withNamespace(namespace)
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
                JobUtils.deleteJobWithWait(namespace, OAUTH_CONSUMER_NAME);

                LOGGER.info("Creating new client with new consumer-group and also to point on {} cluster", targetKafkaCluster);
                KafkaOauthExampleClients kafkaOauthClientJob = new KafkaOauthExampleClients.Builder()
                    .withNamespaceName(namespace)
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
                    ClientUtils.waitForClientSuccess(OAUTH_CONSUMER_NAME, namespace, MESSAGE_COUNT);
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
    void testProducerConsumerMirrorMaker2(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String producerName = OAUTH_PRODUCER_NAME + "-" + clusterName;
        String consumerName = OAUTH_CONSUMER_NAME + "-" + clusterName;
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        KafkaOauthExampleClients oauthExampleClients = new KafkaOauthExampleClients.Builder()
            .withNamespaceName(namespace)
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withOAuthClientId(OAUTH_CLIENT_NAME)
            .withOAuthClientSecret(OAUTH_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, topicName, namespace).build());
        resourceManager.createResource(extensionContext, oauthExampleClients.producerStrimziOauthPlain().build());
        ClientUtils.waitForClientSuccess(producerName, namespace, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(namespace, producerName);

        resourceManager.createResource(extensionContext, oauthExampleClients.consumerStrimziOauthPlain().build());
        ClientUtils.waitForClientSuccess(consumerName, namespace, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(namespace, consumerName);

        String kafkaSourceClusterName = oauthClusterName;
        String kafkaTargetClusterName = clusterName + "-target";
        // mirror maker 2 adding prefix to mirrored topic for in this case mirrotopic will be : my-cluster.my-topic
        String kafkaTargetClusterTopicName = kafkaSourceClusterName + "." + topicName;

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaTargetClusterName, 1, 1)
            .editMetadata()
                .withNamespace(namespace)
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
                .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
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
                .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                .withClientId("kafka-mirror-maker-2")
                .withNewClientSecret()
                    .withSecretName(MIRROR_MAKER_2_OAUTH_SECRET)
                    .withKey(OAUTH_KEY)
                .endClientSecret()
            .endKafkaClientAuthenticationOAuth()
            .build();

        resourceManager.createResource(extensionContext, KafkaMirrorMaker2Templates.kafkaMirrorMaker2(oauthClusterName, kafkaTargetClusterName, kafkaSourceClusterName, 1, false)
            .editMetadata()
                .withNamespace(namespace)
            .endMetadata()
            .editSpec()
                .withClusters(sourceClusterWithOauth, targetClusterWithOauth)
                .editFirstMirror()
                    .withSourceCluster(kafkaSourceClusterName)
                .endMirror()
            .endSpec()
            .build());

        TestUtils.waitFor("Waiting for Mirror Maker 2 will copy messages from " + kafkaSourceClusterName + " to " + kafkaTargetClusterName,
            Duration.ofSeconds(30).toMillis(), Constants.TIMEOUT_FOR_MIRROR_MAKER_COPY_MESSAGES_BETWEEN_BROKERS,
            () -> {
                LOGGER.info("Deleting the Job {}", consumerName);
                JobUtils.deleteJobWithWait(namespace, consumerName);

                LOGGER.info("Creating new client with new consumer-group and also to point on {} cluster", kafkaTargetClusterName);

                KafkaOauthExampleClients kafkaOauthClientJob = new KafkaOauthExampleClients.Builder()
                    .withNamespaceName(namespace)
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
                    ClientUtils.waitForClientSuccess(consumerName, namespace, MESSAGE_COUNT);
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
        String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String producerName = OAUTH_PRODUCER_NAME + "-" + clusterName;
        String consumerName = OAUTH_CONSUMER_NAME + "-" + clusterName;
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        KafkaOauthExampleClients oauthExampleClients = new KafkaOauthExampleClients.Builder()
            .withNamespaceName(namespace)
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(oauthClusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withOAuthClientId(OAUTH_CLIENT_NAME)
            .withOAuthClientSecret(OAUTH_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, topicName, namespace).build());
        resourceManager.createResource(extensionContext, oauthExampleClients.producerStrimziOauthPlain().build());
        ClientUtils.waitForClientSuccess(producerName, namespace, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(namespace, producerName);

        resourceManager.createResource(extensionContext, oauthExampleClients.consumerStrimziOauthPlain().build());
        ClientUtils.waitForClientSuccess(consumerName, namespace, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(namespace, consumerName);

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(namespace, false, kafkaClientsName).build());
        resourceManager.createResource(extensionContext, KafkaBridgeTemplates.kafkaBridge(oauthClusterName, KafkaResources.plainBootstrapAddress(oauthClusterName), 1)
            .editMetadata()
                .withNamespace(namespace)
            .endMetadata()
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
            .withNamespaceName(namespace)
            .withProducerName(bridgeProducerName)
            .withBootstrapAddress(KafkaBridgeResources.serviceName(oauthClusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withPort(HTTP_BRIDGE_DEFAULT_PORT)
            .withDelayMs(1000)
            .withPollInterval(1000)
            .build();

        resourceManager.createResource(extensionContext, kafkaBridgeClientJob.producerStrimziBridge().build());
        ClientUtils.waitForClientSuccess(bridgeProducerName, namespace, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(namespace, bridgeProducerName);
    }

    @ParallelTest
    void testSaslPlainAuthenticationKafkaConnectIsAbleToConnectToKafkaOAuth(ExtensionContext extensionContext) {
        TestStorage testStorage = new TestStorage(extensionContext, namespace);

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, testStorage.getKafkaClientsName()).build());
        resourceManager.createResource(extensionContext, false, KafkaConnectTemplates.kafkaConnect(extensionContext, testStorage.getClusterName(), testStorage.getNamespaceName(), oauthClusterName, 1)
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
        System.out.println("==================<->>>>" + testStorage.getNamespaceName());
        KafkaConnectUtils.waitForConnectReady(testStorage.getNamespaceName(), testStorage.getClusterName());
    }

    @BeforeAll
    void setUp(ExtensionContext extensionContext) {
        // for namespace
        keycloakInstance = super.setupCoAndKeycloak(extensionContext, namespace, keycloakInstance);

        final String customClaimListener = "cclistener";
        final String audienceListener = "audlistnr";

        keycloakInstance.setRealm("internal", false);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(oauthClusterName, 1, 1)
            .editMetadata()
                .withNamespace(namespace)
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
                                    .withJwksExpirySeconds(keycloakInstance.getJwksExpireSeconds())
                                    .withJwksRefreshSeconds(keycloakInstance.getJwksRefreshSeconds())
                                    .withJwksEndpointUri(keycloakInstance.getJwksEndpointUri())
                                    .withUserNameClaim(keycloakInstance.getUserNameClaim())
                                    .withEnablePlain(true)
                                    .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
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
                .endKafka()
            .endSpec()
            .build());
    }

    @AfterAll
    void tearDown(ExtensionContext extensionContext) throws Exception {
        // delete keycloak before namespace
        KeycloakUtils.deleteKeycloakWithoutCRDs(namespace);
        super.deleteKeycloakCRDsIfPossible(extensionContext);
        // delete namespace etc.
        super.afterAllMayOverride(extensionContext);
    }
}
