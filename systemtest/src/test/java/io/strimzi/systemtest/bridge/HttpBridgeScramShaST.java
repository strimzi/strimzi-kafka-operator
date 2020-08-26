/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.PasswordSecretSource;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.utils.ClientUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(INTERNAL_CLIENTS_USED)
@Tag(BRIDGE)
@Tag(REGRESSION)
class HttpBridgeScramShaST extends HttpBridgeAbstractST {
    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeScramShaST.class);
    private static final String NAMESPACE = "bridge-scram-sha-cluster-test";

    private String kafkaClientsPodName;

    @Test
    void testSendSimpleMessageTlsScramSha() {
        // Create topic
        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME).done();

        KafkaClientsResource.producerStrimziBridge(producerName, bridgeServiceName, bridgePort, TOPIC_NAME, MESSAGE_COUNT).done();
        ClientUtils.waitForClientSuccess(producerName, NAMESPACE, MESSAGE_COUNT);

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(USER_NAME)
            .withUsingPodName(kafkaClientsPodName)
            .withSecurityProtocol(SecurityProtocol.SASL_SSL)
            .build();

        assertThat(internalKafkaClient.receiveMessagesTls(), is(MESSAGE_COUNT));
    }

    @Test
    void testReceiveSimpleMessageTlsScramSha() {
        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME).done();

        KafkaClientsResource.consumerStrimziBridge(consumerName, bridgeServiceName, bridgePort, TOPIC_NAME, MESSAGE_COUNT).done();

        // Send messages to Kafka
        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(USER_NAME)
            .withUsingPodName(kafkaClientsPodName)
            .withSecurityProtocol(SecurityProtocol.SASL_SSL)
            .build();

        assertThat(internalKafkaClient.sendMessagesTls(), is(MESSAGE_COUNT));

        ClientUtils.waitForClientSuccess(consumerName, NAMESPACE, MESSAGE_COUNT);
    }

    @BeforeAll
    void setup() throws Exception {
        deployClusterOperator(NAMESPACE);
        LOGGER.info("Deploy Kafka and KafkaBridge before tests");

        // Deploy kafka
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 1, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .withNewTls()
                            .withAuth(new KafkaListenerAuthenticationScramSha512())
                        .endTls()
                    .endListeners()
                .endKafka()
            .endSpec().done();

        // Create Kafka user
        KafkaUser scramShaUser = KafkaUserResource.scramShaUser(CLUSTER_NAME, USER_NAME).done();

        KafkaClientsResource.deployKafkaClients(true, KAFKA_CLIENTS_NAME, scramShaUser).done();

        kafkaClientsPodName = kubeClient().listPodsByPrefixInName(KAFKA_CLIENTS_NAME).get(0).getMetadata().getName();

        // Initialize PasswordSecret to set this as PasswordSecret in Mirror Maker spec
        PasswordSecretSource passwordSecret = new PasswordSecretSource();
        passwordSecret.setSecretName(USER_NAME);
        passwordSecret.setPassword("password");

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecret = new CertSecretSource();
        certSecret.setCertificate("ca.crt");
        certSecret.setSecretName(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME));


        // Deploy http bridge
        KafkaBridgeResource.kafkaBridge(CLUSTER_NAME, KafkaResources.tlsBootstrapAddress(CLUSTER_NAME), 1)
            .editSpec()
                .withNewConsumer()
                    .addToConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .endConsumer()
                .withNewKafkaClientAuthenticationScramSha512()
                    .withNewUsername(USER_NAME)
                    .withPasswordSecret(passwordSecret)
                .endKafkaClientAuthenticationScramSha512()
                .withNewTls()
                    .withTrustedCertificates(certSecret)
                .endTls()
            .endSpec()
            .done();
    }
}
