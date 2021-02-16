/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.oauth;

import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaMirrorMakerResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.keycloak.KeycloakInstance;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMakerResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBridgeExampleClients;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaOauthExampleClients;
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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.HTTP_BRIDGE_DEFAULT_PORT;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
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

    private KafkaOauthExampleClients oauthInternalClientJob;
    private final String oauthClusterName = "oauth-cluster-tls-name";

    @Description(
            "As an oauth producer, I am able to produce messages to the kafka broker\n" +
            "As an oauth consumer, I am able to consumer messages from the kafka broker using encrypted communication")
    @Test
    void testProducerConsumer() {
        oauthInternalClientJob.createAndWaitForReadiness(oauthInternalClientJob.producerStrimziOauthTls(oauthClusterName).build());
        ClientUtils.waitForClientSuccess(OAUTH_PRODUCER_NAME, NAMESPACE, MESSAGE_COUNT);

        oauthInternalClientJob.createAndWaitForReadiness(oauthInternalClientJob.consumerStrimziOauthTls(oauthClusterName).build());
        ClientUtils.waitForClientSuccess(OAUTH_CONSUMER_NAME, NAMESPACE, MESSAGE_COUNT);
    }

    @Description("As an oauth KafkaConnect, I am able to sink messages from kafka broker topic using encrypted communication.")
    @Test
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testProducerConsumerConnect() {
        oauthInternalClientJob.createAndWaitForReadiness(oauthInternalClientJob.producerStrimziOauthTls(oauthClusterName).build());
        ClientUtils.waitForClientSuccess(OAUTH_PRODUCER_NAME, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, OAUTH_PRODUCER_NAME);

        oauthInternalClientJob.createAndWaitForReadiness(oauthInternalClientJob.consumerStrimziOauthTls(oauthClusterName).build());
        ClientUtils.waitForClientSuccess(OAUTH_CONSUMER_NAME, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, OAUTH_CONSUMER_NAME);

        KafkaClientsResource.createAndWaitForReadiness(KafkaClientsResource.deployKafkaClients(false, oauthClusterName + "-" + Constants.KAFKA_CLIENTS).build());

        String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(oauthClusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        KafkaConnectResource.createAndWaitForReadiness(KafkaConnectResource.kafkaConnect(oauthClusterName, 1)
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
                            .withSecretName(oauthClusterName + "-cluster-ca-cert")
                            .withCertificate("ca.crt")
                        .endTrustedCertificate()
                    .endTls()
                    .withBootstrapServers(oauthClusterName + "-kafka-bootstrap:9093")
                .endSpec()
                .build());

        String kafkaConnectPodName = kubeClient().listPods(Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();

        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(kafkaConnectPodName);

        KafkaConnectorUtils.createFileSinkConnector(defaultKafkaClientsPodName, TOPIC_NAME, Constants.DEFAULT_SINK_FILE_PATH, KafkaConnectResources.url(oauthClusterName, NAMESPACE, 8083));

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(kafkaConnectPodName, Constants.DEFAULT_SINK_FILE_PATH);
    }

    @Description("As a oauth bridge, i am able to send messages to bridge endpoint using encrypted communication")
    @Test
    @Tag(BRIDGE)
    void testProducerConsumerBridge() {
        oauthInternalClientJob.createAndWaitForReadiness(oauthInternalClientJob.producerStrimziOauthTls(oauthClusterName).build());
        ClientUtils.waitForClientSuccess(OAUTH_PRODUCER_NAME, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, OAUTH_PRODUCER_NAME);

        oauthInternalClientJob.createAndWaitForReadiness(oauthInternalClientJob.consumerStrimziOauthTls(oauthClusterName).build());
        ClientUtils.waitForClientSuccess(OAUTH_CONSUMER_NAME, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, OAUTH_CONSUMER_NAME);

        KafkaClientsResource.createAndWaitForReadiness(KafkaClientsResource.deployKafkaClients(true, kafkaClientsName).build());

        KafkaBridgeResource.createAndWaitForReadiness(KafkaBridgeResource.kafkaBridge(oauthClusterName, KafkaResources.tlsBootstrapAddress(oauthClusterName), 1)
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

        String producerName = "bridge-producer";

        KafkaBridgeExampleClients kafkaBridgeClientJob = new KafkaBridgeExampleClients.Builder()
            .withProducerName(producerName)
            .withBootstrapAddress(KafkaBridgeResources.serviceName(oauthClusterName))
            .withTopicName(TOPIC_NAME)
            .withMessageCount(10)
            .withPort(HTTP_BRIDGE_DEFAULT_PORT)
            .withDelayMs(1000)
            .withPollInterval(1000)
            .build();

        kafkaBridgeClientJob.createAndWaitForReadiness(kafkaBridgeClientJob.producerStrimziBridge().build());
        ClientUtils.waitForClientSuccess(producerName, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, producerName);
    }

    @Description("As a oauth mirror maker, I am able to replicate topic data using using encrypted communication")
    @Test
    @Tag(MIRROR_MAKER)
    @Tag(NODEPORT_SUPPORTED)
    void testMirrorMaker() {
        oauthInternalClientJob.createAndWaitForReadiness(oauthInternalClientJob.producerStrimziOauthTls(oauthClusterName).build());
        ClientUtils.waitForClientSuccess(OAUTH_PRODUCER_NAME, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, OAUTH_PRODUCER_NAME);

        oauthInternalClientJob.createAndWaitForReadiness(oauthInternalClientJob.consumerStrimziOauthTls(oauthClusterName).build());
        ClientUtils.waitForClientSuccess(OAUTH_CONSUMER_NAME, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, OAUTH_CONSUMER_NAME);

        String targetKafkaCluster = oauthClusterName + "-target";

        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(targetKafkaCluster, 1, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
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
                        .endGenericKafkaListener()
                        .addNewGenericKafkaListener()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
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
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        KafkaMirrorMakerResource.createAndWaitForReadiness(KafkaMirrorMakerResource.kafkaMirrorMaker(oauthClusterName, oauthClusterName, targetKafkaCluster,
            ClientUtils.generateRandomConsumerGroup(), 1, true)
                .editSpec()
                    .withNewConsumer()
                        // this is for kafka tls connection
                        .withNewTls()
                            .withTrustedCertificates(new CertSecretSourceBuilder()
                                .withCertificate("ca.crt")
                                .withSecretName(KafkaResources.clusterCaCertificateSecretName(oauthClusterName))
                                .build())
                        .endTls()
                        .withBootstrapServers(KafkaResources.tlsBootstrapAddress(oauthClusterName))
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
                .build());

        String mirrorMakerPodName = kubeClient().listPodsByPrefixInName(KafkaMirrorMakerResources.deploymentName(oauthClusterName)).get(0).getMetadata().getName();
        String kafkaMirrorMakerLogs = kubeClient().logs(mirrorMakerPodName);

        assertThat(kafkaMirrorMakerLogs,
            not(containsString("keytool error: java.io.FileNotFoundException: /opt/kafka/consumer-oauth-certs/**/* (No such file or directory)")));

        KafkaUserResource.createAndWaitForReadiness(KafkaUserResource.tlsUser(oauthClusterName, USER_NAME).build());
        KafkaUserUtils.waitForKafkaUserCreation(USER_NAME);

        LOGGER.info("Creating new client with new consumer-group and also to point on {} cluster", targetKafkaCluster);

        KafkaOauthExampleClients kafkaOauthClientJob = new KafkaOauthExampleClients.Builder()
            .withProducerName(OAUTH_PRODUCER_NAME)
            .withConsumerName(OAUTH_CONSUMER_NAME)
            .withUserName(USER_NAME)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(targetKafkaCluster))
            .withTopicName(TOPIC_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withOAuthClientId(OAUTH_CLIENT_NAME)
            .withOAuthClientSecret(OAUTH_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        kafkaOauthClientJob.createAndWaitForReadiness(kafkaOauthClientJob.consumerStrimziOauthTls(targetKafkaCluster).build());

        ClientUtils.waitForClientSuccess(OAUTH_CONSUMER_NAME, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, OAUTH_CONSUMER_NAME);
    }

    @Test
    void testIntrospectionEndpoint() {
        LOGGER.info("Deploying kafka...");

        keycloakInstance.setIntrospectionEndpointUri("https://" + keycloakInstance.getHttpsUri() + "/auth/realms/internal/protocol/openid-connect/token/introspect");
        String introspectionKafka = oauthClusterName + "-intro";

        CertSecretSource cert = new CertSecretSourceBuilder()
                .withNewSecretName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                .withNewCertificate(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                .build();

        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(introspectionKafka, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                    .addNewGenericKafkaListener()
                        .withName("tls")
                        .withPort(9093)
                        .withType(KafkaListenerType.INTERNAL)
                        .withTls(true)
                        .withNewKafkaListenerAuthenticationOAuth()
                            .withClientId(OAUTH_KAFKA_CLIENT_NAME)
                            .withNewClientSecret()
                                .withSecretName(OAUTH_KAFKA_CLIENT_SECRET)
                                .withKey(OAUTH_KEY)
                            .endClientSecret()
                            .withAccessTokenIsJwt(false)
                            .withValidIssuerUri(keycloakInstance.getValidIssuerUri())
                            .withIntrospectionEndpointUri(keycloakInstance.getIntrospectionEndpointUri())
                            .withTlsTrustedCertificates(cert)
                            .withDisableTlsHostnameVerification(true)
                        .endKafkaListenerAuthenticationOAuth()
                    .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());


        KafkaOauthExampleClients oauthInternalClientIntrospectionJob = new KafkaOauthExampleClients.Builder()
                .withProducerName(OAUTH_PRODUCER_NAME)
                .withConsumerName(OAUTH_CONSUMER_NAME)
                .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(introspectionKafka))
                .withTopicName(TOPIC_NAME)
                .withMessageCount(MESSAGE_COUNT)
                .withOAuthClientId(OAUTH_CLIENT_NAME)
                .withOAuthClientSecret(OAUTH_CLIENT_SECRET)
                .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                .build();

        oauthInternalClientIntrospectionJob.createAndWaitForReadiness(oauthInternalClientIntrospectionJob.producerStrimziOauthTls(introspectionKafka).build());
        ClientUtils.waitForClientSuccess(OAUTH_PRODUCER_NAME, NAMESPACE, MESSAGE_COUNT);

        oauthInternalClientIntrospectionJob.createAndWaitForReadiness(oauthInternalClientIntrospectionJob.consumerStrimziOauthTls(introspectionKafka).build());
        ClientUtils.waitForClientSuccess(OAUTH_CONSUMER_NAME, NAMESPACE, MESSAGE_COUNT);
    }

    @BeforeAll
    void setUp() {
        setupCoAndKeycloak();
        keycloakInstance.setRealm("internal", true);

        LOGGER.info("Keycloak settings {}", keycloakInstance.toString());

        oauthInternalClientJob = new KafkaOauthExampleClients.Builder()
            .withProducerName(OAUTH_PRODUCER_NAME)
            .withConsumerName(OAUTH_CONSUMER_NAME)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(oauthClusterName))
            .withTopicName(TOPIC_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withOAuthClientId(OAUTH_CLIENT_NAME)
            .withOAuthClientSecret(OAUTH_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(oauthClusterName, 3)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
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
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(oauthClusterName, TOPIC_NAME).build());

        KafkaUserResource.createAndWaitForReadiness(KafkaUserResource.tlsUser(oauthClusterName, OAUTH_CLIENT_NAME).build());
    }
}



