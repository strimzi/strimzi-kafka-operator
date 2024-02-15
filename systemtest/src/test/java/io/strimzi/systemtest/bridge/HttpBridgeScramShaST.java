/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.strimzi.api.kafka.model.bridge.KafkaBridgeResources;
import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.api.kafka.model.common.PasswordSecretSource;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClients;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClientsBuilder;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.Random;

import static io.strimzi.systemtest.TestConstants.BRIDGE;
import static io.strimzi.systemtest.TestConstants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.TestConstants.REGRESSION;

@Tag(INTERNAL_CLIENTS_USED)
@Tag(BRIDGE)
@Tag(REGRESSION)
class HttpBridgeScramShaST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeScramShaST.class);
    private final String httpBridgeScramShaClusterName = "http-bridge-scram-sha-cluster-name";
    private BridgeClients kafkaBridgeClientJob;
    private final String kafkaUserName = KafkaUserUtils.generateRandomNameOfKafkaUser();

    @ParallelTest
    void testSendSimpleMessageTlsScramSha() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final String producerName = "producer-" + new Random().nextInt(Integer.MAX_VALUE);
        final String consumerName = "consumer-" + new Random().nextInt(Integer.MAX_VALUE);

        final String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
        final BridgeClients kafkaBridgeClientJb = new BridgeClientsBuilder(kafkaBridgeClientJob)
            .withTopicName(topicName)
            .withProducerName(producerName)
            .build();

        // Create topic
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(httpBridgeScramShaClusterName, topicName, Environment.TEST_SUITE_NAMESPACE).build());

        resourceManager.createResourceWithWait(kafkaBridgeClientJb.producerStrimziBridge());
        ClientUtils.waitForClientSuccess(producerName, Environment.TEST_SUITE_NAMESPACE, testStorage.getMessageCount());

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(topicName)
            .withMessageCount(testStorage.getMessageCount())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(httpBridgeScramShaClusterName))
            .withConsumerName(consumerName)
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withUsername(kafkaUserName)
            .build();

        resourceManager.createResourceWithWait(kafkaClients.consumerScramShaTlsStrimzi(httpBridgeScramShaClusterName));
        ClientUtils.waitForClientSuccess(consumerName, Environment.TEST_SUITE_NAMESPACE, testStorage.getMessageCount());
    }

    @ParallelTest
    void testReceiveSimpleMessageTlsScramSha() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final String producerName = "producer-" + new Random().nextInt(Integer.MAX_VALUE);
        final String consumerName = "consumer-" + new Random().nextInt(Integer.MAX_VALUE);

        final String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
        final BridgeClients kafkaBridgeClientJb = new BridgeClientsBuilder(kafkaBridgeClientJob)
            .withTopicName(topicName)
            .withConsumerName(consumerName)
            .build();

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(httpBridgeScramShaClusterName, testStorage.getTopicName(), Environment.TEST_SUITE_NAMESPACE).build());
        resourceManager.createResourceWithWait(kafkaBridgeClientJb.consumerStrimziBridge());

        // Send messages to Kafka
        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(topicName)
            .withMessageCount(testStorage.getMessageCount())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(httpBridgeScramShaClusterName))
            .withProducerName(producerName)
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withUsername(kafkaUserName)
            .build();

        resourceManager.createResourceWithWait(kafkaClients.producerScramShaTlsStrimzi(httpBridgeScramShaClusterName));
        ClientUtils.waitForClientsSuccess(producerName, consumerName, Environment.TEST_SUITE_NAMESPACE, testStorage.getMessageCount());
    }

    @BeforeAll
    void setUp() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        clusterOperator = clusterOperator.defaultInstallation()
            .createInstallation()
            .runInstallation();

        LOGGER.info("Deploying Kafka and KafkaBridge before tests");

        // Deploy kafka
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(httpBridgeScramShaClusterName, 1, 1)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewKafkaListenerAuthenticationScramSha512Auth()
                            .endKafkaListenerAuthenticationScramSha512Auth()
                            .build())
                .endKafka()
            .endSpec().build());

        // Create Kafka user
        KafkaUser scramShaUser = KafkaUserTemplates.scramShaUser(Environment.TEST_SUITE_NAMESPACE, httpBridgeScramShaClusterName, kafkaUserName)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .build();

        resourceManager.createResourceWithWait(scramShaUser);

        // Initialize PasswordSecret to set this as PasswordSecret in MirrorMaker spec
        PasswordSecretSource passwordSecret = new PasswordSecretSource();
        passwordSecret.setSecretName(kafkaUserName);
        passwordSecret.setPassword("password");

        // Initialize CertSecretSource with certificate and Secret names for consumer
        CertSecretSource certSecret = new CertSecretSource();
        certSecret.setCertificate("ca.crt");
        certSecret.setSecretName(KafkaResources.clusterCaCertificateSecretName(httpBridgeScramShaClusterName));

        // Deploy http bridge
        resourceManager.createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(httpBridgeScramShaClusterName,
            KafkaResources.tlsBootstrapAddress(httpBridgeScramShaClusterName), 1)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .editSpec()
                    .withNewConsumer()
                        .addToConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                    .endConsumer()
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername(kafkaUserName)
                        .withPasswordSecret(passwordSecret)
                    .endKafkaClientAuthenticationScramSha512()
                    .withNewTls()
                        .withTrustedCertificates(certSecret)
                    .endTls()
                .endSpec().build());

        kafkaBridgeClientJob = new BridgeClientsBuilder()
            .withBootstrapAddress(KafkaBridgeResources.serviceName(httpBridgeScramShaClusterName))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withPort(TestConstants.HTTP_BRIDGE_DEFAULT_PORT)
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .build();
    }
}
