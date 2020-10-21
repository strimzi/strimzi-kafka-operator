/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
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

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
@Tag(BRIDGE)
@Tag(ACCEPTANCE)
@Tag(INTERNAL_CLIENTS_USED)
class HttpBridgeTlsST extends HttpBridgeAbstractST {
    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeTlsST.class);
    private static final String NAMESPACE = "bridge-tls-cluster-test";

    @Test
    void testSendSimpleMessageTls() {
        // Create topic
        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME).done();

        KafkaClientsResource.producerStrimziBridge(producerName, bridgeServiceName, bridgePort, TOPIC_NAME, MESSAGE_COUNT).done();
        ClientUtils.waitForClientSuccess(producerName, NAMESPACE, MESSAGE_COUNT);

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withKafkaUsername(USER_NAME)
            .withUsingPodName(kafkaClientsPodName)
            .build();

        assertThat(internalKafkaClient.receiveMessagesTls(), is(MESSAGE_COUNT));
    }

    @Test
    void testReceiveSimpleMessageTls() {
        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME).done();

        KafkaClientsResource.consumerStrimziBridge(consumerName, bridgeServiceName, bridgePort, TOPIC_NAME, MESSAGE_COUNT).done();

        // Send messages to Kafka
        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withKafkaUsername(USER_NAME)
            .withUsingPodName(kafkaClientsPodName)
            .build();

        assertThat(internalKafkaClient.sendMessagesTls(), is(MESSAGE_COUNT));

        ClientUtils.waitForClientSuccess(consumerName, NAMESPACE, MESSAGE_COUNT);
    }

    @BeforeAll
    void createClassResources() throws Exception {
        deployClusterOperator(NAMESPACE);
        LOGGER.info("Deploy Kafka and KafkaBridge before tests");

        // Deploy kafka
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 1, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .withNewTls()
                            .withAuth(new KafkaListenerAuthenticationTls())
                        .endTls()
                    .endListeners()
                .endKafka()
            .endSpec()
            .done();

        // Create Kafka user
        KafkaUser tlsUser = KafkaUserResource.tlsUser(CLUSTER_NAME, USER_NAME).done();

        KafkaClientsResource.deployKafkaClients(true, KAFKA_CLIENTS_NAME, tlsUser).done();

        kafkaClientsPodName = kubeClient().listPodsByPrefixInName(KAFKA_CLIENTS_NAME).get(0).getMetadata().getName();

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
                .withNewKafkaClientAuthenticationTls()
                    .withNewCertificateAndKey()
                        .withSecretName(USER_NAME)
                        .withCertificate("user.crt")
                        .withKey("user.key")
                    .endCertificateAndKey()
                .endKafkaClientAuthenticationTls()
                .withNewTls()
                    .withTrustedCertificates(certSecret)
                .endTls()
            .endSpec()
            .done();
    }
}
