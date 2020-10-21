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
import io.strimzi.api.kafka.model.listener.KafkaListenerExternalNodePortBuilder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.externalClients.OauthExternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMakerResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.vertx.core.cli.annotations.Description;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
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
import static io.strimzi.systemtest.Constants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.HTTP_BRIDGE_DEFAULT_PORT;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.OAUTH;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(OAUTH)
@Tag(REGRESSION)
@Tag(NODEPORT_SUPPORTED)
@Tag(EXTERNAL_CLIENTS_USED)
@Tag(ACCEPTANCE)
public class OauthTlsST extends OauthAbstractST {

    private OauthExternalKafkaClient oauthExternalKafkaClientTls;

    @Description(
            "As an oauth producer, I am able to produce messages to the kafka broker\n" +
            "As an oauth consumer, I am able to consumer messages from the kafka broker using encrypted communication")
    @Test
    void testProducerConsumer() {
        oauthExternalKafkaClientTls.verifyProducedAndConsumedMessages(
            oauthExternalKafkaClientTls.sendMessagesTls(),
            oauthExternalKafkaClientTls.receiveMessagesTls()
        );
    }

    @Description("As an oauth KafkaConnect, I am able to sink messages from kafka broker topic using encrypted communication.")
    @Test
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testProducerConsumerConnect() {
        oauthExternalKafkaClientTls.verifyProducedAndConsumedMessages(
            oauthExternalKafkaClientTls.sendMessagesTls(),
            oauthExternalKafkaClientTls.receiveMessagesTls()
        );

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
                                .withSecretName(SECRET_OF_KEYCLOAK)
                                .withCertificate(CERTIFICATE_OF_KEYCLOAK)
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
        oauthExternalKafkaClientTls.verifyProducedAndConsumedMessages(
            oauthExternalKafkaClientTls.sendMessagesTls(),
            oauthExternalKafkaClientTls.receiveMessagesTls()
        );

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
                            .withSecretName(SECRET_OF_KEYCLOAK)
                            .withCertificate(CERTIFICATE_OF_KEYCLOAK)
                        .endTlsTrustedCertificate()
                        .withDisableTlsHostnameVerification(true)
                    .endKafkaClientAuthenticationOAuth()
                .endSpec()
                .done();

        String producerName = "bridge-producer";

        KafkaClientsResource.producerStrimziBridge(producerName, KafkaBridgeResources.serviceName(CLUSTER_NAME), HTTP_BRIDGE_DEFAULT_PORT, TOPIC_NAME, MESSAGE_COUNT).done();
        ClientUtils.waitForClientSuccess(producerName, NAMESPACE, MESSAGE_COUNT);
    }

    @Description("As a oauth mirror maker, i am able to replicate topic data using using encrypted communication")
    @Test
    @Tag(MIRROR_MAKER)
    void testMirrorMaker() {
        oauthExternalKafkaClientTls.verifyProducedAndConsumedMessages(
            oauthExternalKafkaClientTls.sendMessagesTls(),
            oauthExternalKafkaClientTls.receiveMessagesTls()
        );

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
                                        .withSecretName(SECRET_OF_KEYCLOAK)
                                        .withCertificate(CERTIFICATE_OF_KEYCLOAK)
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
                                        .withSecretName(SECRET_OF_KEYCLOAK)
                                        .withCertificate(CERTIFICATE_OF_KEYCLOAK)
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
                                .withSecretName(SECRET_OF_KEYCLOAK)
                                .withCertificate(CERTIFICATE_OF_KEYCLOAK)
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
                                .withSecretName(SECRET_OF_KEYCLOAK)
                                .withCertificate(CERTIFICATE_OF_KEYCLOAK)
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

        oauthExternalKafkaClientTls.setClusterName(targetKafkaCluster);
        oauthExternalKafkaClientTls.setConsumerGroup(ClientUtils.generateRandomConsumerGroup());

        assertThat(oauthExternalKafkaClientTls.receiveMessagesTls(), is(MESSAGE_COUNT));
    }

    @BeforeEach
    void setUpEach() {
        oauthExternalKafkaClientTls.setConsumerGroup(ClientUtils.generateRandomConsumerGroup());
    }

    @BeforeAll
    void setUp() {
        keycloakInstance.setRealm("internal", true);

        LOGGER.info("Keycloak settings {}", keycloakInstance.toString());

        oauthExternalKafkaClientTls = new OauthExternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withKafkaUsername(OAUTH_CLIENT_NAME)
            .withClientSecretName(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        LOGGER.info("Oauth kafka client has following settings {}", oauthExternalKafkaClientTls.toString());

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            kafka.getSpec().getKafka().getListeners().setExternal(
                new KafkaListenerExternalNodePortBuilder()
                    .withNewKafkaListenerAuthenticationOAuth()
                        .withValidIssuerUri(keycloakInstance.getValidIssuerUri())
                        .withJwksEndpointUri(keycloakInstance.getJwksEndpointUri())
                        .withJwksExpirySeconds(keycloakInstance.getJwksExpireSeconds())
                        .withJwksRefreshSeconds(keycloakInstance.getJwksRefreshSeconds())
                        .withUserNameClaim(keycloakInstance.getUserNameClaim())
                        .withTlsTrustedCertificates(
                            new CertSecretSourceBuilder()
                                .withSecretName(SECRET_OF_KEYCLOAK)
                                .withCertificate(CERTIFICATE_OF_KEYCLOAK)
                                .build())
                        .withDisableTlsHostnameVerification(true)
                    .endKafkaListenerAuthenticationOAuth()
                    .build());
        });

        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3);
    }
}



