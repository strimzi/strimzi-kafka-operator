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
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
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
    private BridgeClients kafkaBridgeClientJob;
    private TestStorage suiteTestStorage;
    
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
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(suiteTestStorage.getClusterName(), topicName, Environment.TEST_SUITE_NAMESPACE).build());

        resourceManager.createResourceWithWait(kafkaBridgeClientJb.producerStrimziBridge());
        ClientUtils.waitForClientSuccess(producerName, Environment.TEST_SUITE_NAMESPACE, testStorage.getMessageCount());

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(topicName)
            .withMessageCount(testStorage.getMessageCount())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(suiteTestStorage.getClusterName()))
            .withConsumerName(consumerName)
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withUsername(suiteTestStorage.getUsername())
            .build();

        resourceManager.createResourceWithWait(kafkaClients.consumerScramShaTlsStrimzi(suiteTestStorage.getClusterName()));
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

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(suiteTestStorage.getClusterName(), testStorage.getTopicName(), Environment.TEST_SUITE_NAMESPACE).build());
        resourceManager.createResourceWithWait(kafkaBridgeClientJb.consumerStrimziBridge());

        // Send messages to Kafka
        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(topicName)
            .withMessageCount(testStorage.getMessageCount())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(suiteTestStorage.getClusterName()))
            .withProducerName(producerName)
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withUsername(suiteTestStorage.getUsername())
            .build();

        resourceManager.createResourceWithWait(kafkaClients.producerScramShaTlsStrimzi(suiteTestStorage.getClusterName()));
        ClientUtils.waitForClientsSuccess(producerName, consumerName, Environment.TEST_SUITE_NAMESPACE, testStorage.getMessageCount());
    }

    @BeforeAll
    void setUp() {
        suiteTestStorage = new TestStorage(ResourceManager.getTestContext());

        clusterOperator = clusterOperator.defaultInstallation()
            .createInstallation()
            .runInstallation();

        LOGGER.info("Deploying Kafka and KafkaBridge before tests");

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(suiteTestStorage.getNamespaceName(), suiteTestStorage.getBrokerPoolName(), suiteTestStorage.getClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPool(suiteTestStorage.getNamespaceName(), suiteTestStorage.getControllerPoolName(), suiteTestStorage.getClusterName(), 3).build()
            )
        );
        // Deploy kafka
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(suiteTestStorage.getClusterName(), 1, 3)
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
        KafkaUser scramShaUser = KafkaUserTemplates.scramShaUser(Environment.TEST_SUITE_NAMESPACE, suiteTestStorage.getClusterName(), suiteTestStorage.getUsername())
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .build();

        resourceManager.createResourceWithWait(scramShaUser);

        // Initialize PasswordSecret to set this as PasswordSecret in MirrorMaker spec
        PasswordSecretSource passwordSecret = new PasswordSecretSource();
        passwordSecret.setSecretName(suiteTestStorage.getUsername());
        passwordSecret.setPassword("password");

        // Initialize CertSecretSource with certificate and Secret names for consumer
        CertSecretSource certSecret = new CertSecretSource();
        certSecret.setCertificate("ca.crt");
        certSecret.setSecretName(KafkaResources.clusterCaCertificateSecretName(suiteTestStorage.getClusterName()));

        // Deploy http bridge
        resourceManager.createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(suiteTestStorage.getClusterName(),
            KafkaResources.tlsBootstrapAddress(suiteTestStorage.getClusterName()), 1)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .editSpec()
                    .withNewConsumer()
                        .addToConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                    .endConsumer()
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername(suiteTestStorage.getUsername())
                        .withPasswordSecret(passwordSecret)
                    .endKafkaClientAuthenticationScramSha512()
                    .withNewTls()
                        .withTrustedCertificates(certSecret)
                    .endTls()
                .endSpec().build());

        kafkaBridgeClientJob = new BridgeClientsBuilder()
            .withBootstrapAddress(KafkaBridgeResources.serviceName(suiteTestStorage.getClusterName()))
            .withComponentName(KafkaBridgeResources.componentName(suiteTestStorage.getClusterName()))
            .withTopicName(suiteTestStorage.getTopicName())
            .withMessageCount(suiteTestStorage.getMessageCount())
            .withPort(TestConstants.HTTP_BRIDGE_DEFAULT_PORT)
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .build();
    }
}
