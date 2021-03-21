/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.PasswordSecretSource;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBridgeExampleClients;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

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
    private final String httpBridgeScramShaClusterName = "http-bridge-scram-sha-cluster-name";

    private String kafkaClientsPodName;

    @ParallelTest
    void testSendSimpleMessageTlsScramSha(ExtensionContext extensionContext) {
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
        KafkaBridgeExampleClients kafkaBridgeClientJb = kafkaBridgeClientJob.toBuilder().withTopicName(topicName).build();

        // Create topic
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(httpBridgeScramShaClusterName, topicName).build());
        resourceManager.createResource(extensionContext, kafkaBridgeClientJb.producerStrimziBridge().build());

        ClientUtils.waitForClientSuccess(producerName, NAMESPACE, MESSAGE_COUNT);

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(httpBridgeScramShaClusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(USER_NAME)
            .withUsingPodName(kafkaClientsPodName)
            .withSecurityProtocol(SecurityProtocol.SASL_SSL)
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        assertThat(internalKafkaClient.receiveMessagesTls(), is(MESSAGE_COUNT));
    }

    @ParallelTest
    void testReceiveSimpleMessageTlsScramSha(ExtensionContext extensionContext) {
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
        KafkaBridgeExampleClients kafkaBridgeClientJb = kafkaBridgeClientJob.toBuilder().withTopicName(topicName).build();

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(httpBridgeScramShaClusterName, TOPIC_NAME).build());
        resourceManager.createResource(extensionContext, kafkaBridgeClientJb.consumerStrimziBridge().build());

        // Send messages to Kafka
        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(httpBridgeScramShaClusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(USER_NAME)
            .withUsingPodName(kafkaClientsPodName)
            .withSecurityProtocol(SecurityProtocol.SASL_SSL)
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        assertThat(internalKafkaClient.sendMessagesTls(), is(MESSAGE_COUNT));

        ClientUtils.waitForClientSuccess(consumerName, NAMESPACE, MESSAGE_COUNT);
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        installClusterOperator(extensionContext, NAMESPACE);
        LOGGER.info("Deploy Kafka and KafkaBridge before tests");

        // Deploy kafka
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(httpBridgeScramShaClusterName, 1, 1)
            .editSpec()
            .editKafka()
                .withNewListeners()
                    .addNewGenericKafkaListener()
                        .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                        .withPort(9093)
                        .withType(KafkaListenerType.INTERNAL)
                        .withTls(true)
                        .withNewKafkaListenerAuthenticationScramSha512Auth()
                        .endKafkaListenerAuthenticationScramSha512Auth()
                    .endGenericKafkaListener()
                .endListeners()
            .endKafka()
            .endSpec().build());

        String kafkaClientsName = NAMESPACE + "-shared-" + Constants.KAFKA_CLIENTS;

        // Create Kafka user
        KafkaUser scramShaUser = KafkaUserTemplates.scramShaUser(httpBridgeScramShaClusterName, USER_NAME).build();

        resourceManager.createResource(extensionContext, scramShaUser);
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, kafkaClientsName, scramShaUser).build());

        kafkaClientsPodName = kubeClient().listPodsByPrefixInName(kafkaClientsName).get(0).getMetadata().getName();

        // Initialize PasswordSecret to set this as PasswordSecret in Mirror Maker spec
        PasswordSecretSource passwordSecret = new PasswordSecretSource();
        passwordSecret.setSecretName(USER_NAME);
        passwordSecret.setPassword("password");

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecret = new CertSecretSource();
        certSecret.setCertificate("ca.crt");
        certSecret.setSecretName(KafkaResources.clusterCaCertificateSecretName(httpBridgeScramShaClusterName));

        // Deploy http bridge
        resourceManager.createResource(extensionContext, KafkaBridgeTemplates.kafkaBridge(httpBridgeScramShaClusterName,
            KafkaResources.tlsBootstrapAddress(httpBridgeScramShaClusterName), 1)
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
                .endSpec().build());

        kafkaBridgeClientJob = kafkaBridgeClientJob.toBuilder().withBootstrapAddress(KafkaBridgeResources.serviceName(httpBridgeScramShaClusterName)).build();
    }
}
