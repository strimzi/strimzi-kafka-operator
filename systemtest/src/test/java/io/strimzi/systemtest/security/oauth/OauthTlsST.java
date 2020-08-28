/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.oauth;

import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaMirrorMakerResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.keycloak.KeycloakInstance;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBridgeClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMakerResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaOauthClientsResource;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.JobUtils;
import io.vertx.core.cli.annotations.Description;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.HTTP_BRIDGE_DEFAULT_PORT;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER;
import static io.strimzi.systemtest.Constants.OAUTH;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(OAUTH)
@Tag(REGRESSION)
@Tag(ACCEPTANCE)
public class OauthTlsST extends OauthAbstractST {
    protected static final Logger LOGGER = LogManager.getLogger(OauthTlsST.class);

    private KafkaOauthClientsResource oauthInternalClientJob;

    @Description(
            "As an oauth producer, I am able to produce messages to the kafka broker\n" +
            "As an oauth consumer, I am able to consumer messages from the kafka broker using encrypted communication")
    @Test
    void testProducerConsumer() {
        oauthInternalClientJob.producerStrimziOauthTls(CLUSTER_NAME).done();
        ClientUtils.waitForClientSuccess(OAUTH_PRODUCER_NAME, NAMESPACE, MESSAGE_COUNT);

        oauthInternalClientJob.consumerStrimziOauthTls(CLUSTER_NAME).done();
        ClientUtils.waitForClientSuccess(OAUTH_CONSUMER_NAME, NAMESPACE, MESSAGE_COUNT);
    }

    @Description("As an oauth KafkaConnect, I am able to sink messages from kafka broker topic using encrypted communication.")
    @Test
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testProducerConsumerConnect() {
        oauthInternalClientJob.producerStrimziOauthTls(CLUSTER_NAME).done();
        ClientUtils.waitForClientSuccess(OAUTH_PRODUCER_NAME, NAMESPACE, MESSAGE_COUNT);

        oauthInternalClientJob.consumerStrimziOauthTls(CLUSTER_NAME).done();
        ClientUtils.waitForClientSuccess(OAUTH_CONSUMER_NAME, NAMESPACE, MESSAGE_COUNT);

        KafkaClientsResource.deployKafkaClients(false, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).done();

        String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        KafkaConnectResource.kafkaConnect(CLUSTER_NAME, 1)
                .editSpec()
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
                            .withSecretName(CLUSTER_NAME + "-cluster-ca-cert")
                            .withCertificate("ca.crt")
                        .endTrustedCertificate()
                    .endTls()
                    .withBootstrapServers(CLUSTER_NAME + "-kafka-bootstrap:9093")
                .endSpec()
                .done();

        String kafkaConnectPodName = kubeClient().listPods(Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();

        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(kafkaConnectPodName);

        KafkaConnectorUtils.createFileSinkConnector(defaultKafkaClientsPodName, TOPIC_NAME, Constants.DEFAULT_SINK_FILE_PATH, KafkaConnectResources.url(CLUSTER_NAME, NAMESPACE, 8083));

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(kafkaConnectPodName, Constants.DEFAULT_SINK_FILE_PATH);
    }

    @Description("As a oauth bridge, i am able to send messages to bridge endpoint using encrypted communication")
    @Test
    @Tag(BRIDGE)
    void testProducerConsumerBridge() {
        oauthInternalClientJob.producerStrimziOauthTls(CLUSTER_NAME).done();
        ClientUtils.waitForClientSuccess(OAUTH_PRODUCER_NAME, NAMESPACE, MESSAGE_COUNT);

        oauthInternalClientJob.consumerStrimziOauthTls(CLUSTER_NAME).done();
        ClientUtils.waitForClientSuccess(OAUTH_CONSUMER_NAME, NAMESPACE, MESSAGE_COUNT);

        KafkaClientsResource.deployKafkaClients(true, KAFKA_CLIENTS_NAME).done();

        KafkaBridgeResource.kafkaBridge(CLUSTER_NAME, KafkaResources.tlsBootstrapAddress(CLUSTER_NAME), 1)
                .editSpec()
                    .withNewTls()
                        .withTrustedCertificates(
                            new CertSecretSourceBuilder()
                                .withCertificate("ca.crt")
                                .withSecretName(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME)).build())
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
                .done();

        String producerName = "bridge-producer";

        KafkaBridgeClientsResource kafkaBridgeClientJob = new KafkaBridgeClientsResource(producerName, "", KafkaBridgeResources.serviceName(CLUSTER_NAME),
            TOPIC_NAME, 10, "", ClientUtils.generateRandomConsumerGroup(), HTTP_BRIDGE_DEFAULT_PORT, 1000, 1000);

        kafkaBridgeClientJob.producerStrimziBridge().done();
        ClientUtils.waitForClientSuccess(producerName, NAMESPACE, MESSAGE_COUNT);
    }

    @Description("As a oauth mirror maker, I am able to replicate topic data using using encrypted communication")
    @Test
    @Tag(MIRROR_MAKER)
    void testMirrorMaker() {
        oauthInternalClientJob.producerStrimziOauthTls(CLUSTER_NAME).done();
        ClientUtils.waitForClientSuccess(OAUTH_PRODUCER_NAME, NAMESPACE, MESSAGE_COUNT);

        oauthInternalClientJob.consumerStrimziOauthTls(CLUSTER_NAME).done();
        ClientUtils.waitForClientSuccess(OAUTH_CONSUMER_NAME, NAMESPACE, MESSAGE_COUNT);

        String targetKafkaCluster = CLUSTER_NAME + "-target";

        KafkaResource.kafkaEphemeral(targetKafkaCluster, 1, 1)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewTls()
                            .withNewKafkaListenerAuthenticationOAuth()
                                .withValidIssuerUri(keycloakInstance.getValidIssuerUri())
                                .withJwksEndpointUri(keycloakInstance.getJwksEndpointUri())
                                .withJwksExpirySeconds(keycloakInstance.getJwksExpireSeconds())
                                .withJwksRefreshSeconds(keycloakInstance.getJwksRefreshSeconds())
                                .withUserNameClaim(keycloakInstance.getUserNameClaim())
                                .withTlsTrustedCertificates(
                                    new CertSecretSourceBuilder()
                                        .withSecretName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                                        .withCertificate(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                                        .build())
                                .withDisableTlsHostnameVerification(true)
                            .endKafkaListenerAuthenticationOAuth()
                        .endTls()
                        .withNewKafkaListenerExternalNodePort()
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
                        .endKafkaListenerExternalNodePort()
                    .endListeners()
                .endKafka()
            .endSpec()
            .done();

        KafkaMirrorMakerResource.kafkaMirrorMaker(CLUSTER_NAME, CLUSTER_NAME, targetKafkaCluster,
            ClientUtils.generateRandomConsumerGroup(), 1, true)
                .editSpec()
                    .withNewConsumer()
                        // this is for kafka tls connection
                        .withNewTls()
                            .withTrustedCertificates(new CertSecretSourceBuilder()
                                .withCertificate("ca.crt")
                                .withSecretName(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME))
                                .build())
                        .endTls()
                        .withBootstrapServers(KafkaResources.tlsBootstrapAddress(CLUSTER_NAME))
                        .withGroupId(ClientUtils.generateRandomConsumerGroup())
                        .addToConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                        .withNewKafkaClientAuthenticationOAuth()
                            .withNewTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                            .withClientId("kafka-mirror-maker")
                            .withNewClientSecret()
                                .withSecretName(MIRROR_MAKER_OAUTH_SECRET)
                                .withKey(OAUTH_KEY)
                            .endClientSecret()
                            // this is for authorization server tls connection
                            .withTlsTrustedCertificates(new CertSecretSourceBuilder()
                                .withSecretName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                                .withCertificate(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                                .build())
                            .withDisableTlsHostnameVerification(true)
                        .endKafkaClientAuthenticationOAuth()
                    .endConsumer()
                    .withNewProducer()
                        .withBootstrapServers(KafkaResources.tlsBootstrapAddress(targetKafkaCluster))
                        // this is for kafka tls connection
                        .withNewTls()
                            .withTrustedCertificates(new CertSecretSourceBuilder()
                                .withCertificate("ca.crt")
                                .withSecretName(KafkaResources.clusterCaCertificateSecretName(targetKafkaCluster))
                                .build())
                        .endTls()
                        .withNewKafkaClientAuthenticationOAuth()
                            .withNewTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                            .withClientId("kafka-mirror-maker")
                            .withNewClientSecret()
                                .withSecretName(MIRROR_MAKER_OAUTH_SECRET)
                                .withKey(OAUTH_KEY)
                            .endClientSecret()
                            // this is for authorization server tls connection
                            .withTlsTrustedCertificates(new CertSecretSourceBuilder()
                                .withSecretName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                                .withCertificate(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                                .build())
                            .withDisableTlsHostnameVerification(true)
                        .endKafkaClientAuthenticationOAuth()
                        .addToConfig(ProducerConfig.ACKS_CONFIG, "all")
                    .endProducer()
                .endSpec()
                .done();

        String mirrorMakerPodName = kubeClient().listPodsByPrefixInName(KafkaMirrorMakerResources.deploymentName(CLUSTER_NAME)).get(0).getMetadata().getName();
        String kafkaMirrorMakerLogs = kubeClient().logs(mirrorMakerPodName);

        assertThat(kafkaMirrorMakerLogs,
            not(containsString("keytool error: java.io.FileNotFoundException: /opt/kafka/consumer-oauth-certs/**/* (No such file or directory)")));

        KafkaUserResource.tlsUser(CLUSTER_NAME, USER_NAME).done();
        KafkaUserUtils.waitForKafkaUserCreation(USER_NAME);

        JobUtils.deleteJob(NAMESPACE, OAUTH_PRODUCER_NAME);

        LOGGER.info("Creating new client with new consumer-group and also to point on {} cluster", targetKafkaCluster);

        KafkaOauthClientsResource kafkaOauthClientJob = new KafkaOauthClientsResource(OAUTH_PRODUCER_NAME, OAUTH_CONSUMER_NAME,
            KafkaResources.tlsBootstrapAddress(targetKafkaCluster), TOPIC_NAME, MESSAGE_COUNT, "",
            ClientUtils.generateRandomConsumerGroup(), OAUTH_CLIENT_NAME, OAUTH_CLIENT_SECRET, keycloakInstance.getOauthTokenEndpointUri());

        kafkaOauthClientJob.consumerStrimziOauthTls(CLUSTER_NAME);

        ClientUtils.waitForClientSuccess(OAUTH_CONSUMER_NAME, NAMESPACE, MESSAGE_COUNT);
    }

    @BeforeEach
    void setUpEach() {
        oauthInternalClientJob = new KafkaOauthClientsResource(oauthInternalClientJob);
    }

    @BeforeAll
    void setUp() {
        keycloakInstance.setRealm("internal", true);

        LOGGER.info("Keycloak settings {}", keycloakInstance.toString());

        oauthInternalClientJob = new KafkaOauthClientsResource(OAUTH_PRODUCER_NAME, OAUTH_CONSUMER_NAME,
            KafkaResources.tlsBootstrapAddress(CLUSTER_NAME), TOPIC_NAME, MESSAGE_COUNT, "",
            ClientUtils.generateRandomConsumerGroup(), OAUTH_CLIENT_NAME, OAUTH_CLIENT_SECRET, keycloakInstance.getOauthTokenEndpointUri());

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewTls()
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
                        .endTls()
                    .endListeners()
                .endKafka()
            .endSpec()
            .done();

        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME).done();

        KafkaUserResource.tlsUser(CLUSTER_NAME, OAUTH_CLIENT_NAME).done();
    }
}



