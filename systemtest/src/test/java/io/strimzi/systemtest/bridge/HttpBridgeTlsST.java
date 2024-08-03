/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.strimzi.api.kafka.model.bridge.KafkaBridgeResources;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeSpec;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeSpecBuilder;
import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.api.kafka.model.common.PasswordSecretSource;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthentication;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClients;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClientsBuilder;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import static io.strimzi.systemtest.TestConstants.ACCEPTANCE;
import static io.strimzi.systemtest.TestConstants.BRIDGE;
import static io.strimzi.systemtest.TestConstants.REGRESSION;

@Tag(REGRESSION)
@Tag(BRIDGE)
@Tag(ACCEPTANCE)
class HttpBridgeTlsST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeTlsST.class);
    private BridgeClients kafkaBridgeClientJob;
    private TestStorage suiteTestStorage;

    @ParallelTest
    void testSendSimpleMessageTls() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        BridgeClients kafkaBridgeClientJobProduce = new BridgeClientsBuilder(kafkaBridgeClientJob)
            .withTopicName(testStorage.getTopicName())
            .withProducerName(testStorage.getProducerName())
            .build();

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), suiteTestStorage.getClusterName(), testStorage.getTopicName()).build());

        resourceManager.createResourceWithWait(kafkaBridgeClientJobProduce.producerStrimziBridge());
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getProducerName(), testStorage.getMessageCount());

        final KafkaClients kafkaClients = ClientUtils.getInstantTlsClientBuilder(testStorage, KafkaResources.tlsBootstrapAddress(suiteTestStorage.getClusterName()))
            .withUsername(suiteTestStorage.getUsername())
            .build();

        resourceManager.createResourceWithWait(kafkaClients.consumerTlsStrimzi(suiteTestStorage.getClusterName()));
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getMessageCount());
    }

    @ParallelTest
    void testReceiveSimpleMessageTls() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        BridgeClients kafkaBridgeClientJobConsume = new BridgeClientsBuilder(kafkaBridgeClientJob)
            .withTopicName(testStorage.getTopicName())
            .withConsumerName(testStorage.getConsumerName())
            .build();

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), suiteTestStorage.getClusterName(), testStorage.getTopicName()).build());

        resourceManager.createResourceWithWait(kafkaBridgeClientJobConsume.consumerStrimziBridge());

        // Send messages to Kafka
        final KafkaClients kafkaClients = ClientUtils.getInstantTlsClientBuilder(testStorage, KafkaResources.tlsBootstrapAddress(suiteTestStorage.getClusterName()))
            .withUsername(suiteTestStorage.getUsername())
            .build();

        resourceManager.createResourceWithWait(kafkaClients.producerTlsStrimzi(suiteTestStorage.getClusterName()));
        ClientUtils.waitForClientsSuccess(testStorage.getNamespaceName(), testStorage.getProducerName(), testStorage.getConsumerName(), testStorage.getMessageCount());
    }

    @ParallelTest
    void testTlsScramShaAuthWithWeirdUsername() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        // Create weird named user with . and more than 64 chars -> SCRAM-SHA
        final String weirdUserName = "jjglmahyijoambryleyxjjglmahy.ijoambryleyxjjglmahyijoambryleyxasd.asdasidioiqweioqiweooioqieioqieoqieooi";

        // Initialize PasswordSecret to set this as PasswordSecret in MirrorMaker spec
        final PasswordSecretSource passwordSecret = new PasswordSecretSource();
        passwordSecret.setSecretName(weirdUserName);
        passwordSecret.setPassword("password");

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecret = new CertSecretSource();
        certSecret.setCertificate("ca.crt");
        certSecret.setSecretName(KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()));

        KafkaBridgeSpec bridgeSpec = new KafkaBridgeSpecBuilder()
            .withNewKafkaClientAuthenticationScramSha512()
                .withUsername(weirdUserName)
                .withPasswordSecret(passwordSecret)
            .endKafkaClientAuthenticationScramSha512()
            .withNewTls()
                .withTrustedCertificates(certSecret)
            .endTls()
            .build();

        testWeirdUsername(weirdUserName, new KafkaListenerAuthenticationScramSha512(), bridgeSpec, testStorage);
    }

    @ParallelTest
    void testTlsAuthWithWeirdUsername() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        // Create weird named user with . and maximum of 64 chars -> TLS
        final String weirdUserName = "jjglmahyijoambryleyxjjglmahy.ijoambryleyxjjglmahyijoambryleyxasd";

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecret = new CertSecretSource();
        certSecret.setCertificate("ca.crt");
        certSecret.setSecretName(KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()));

        KafkaBridgeSpec bridgeSpec = new KafkaBridgeSpecBuilder()
            .withNewKafkaClientAuthenticationTls()
                .withNewCertificateAndKey()
                    .withSecretName(weirdUserName)
                    .withCertificate("user.crt")
                    .withKey("user.key")
                .endCertificateAndKey()
            .endKafkaClientAuthenticationTls()
            .withNewTls()
                .withTrustedCertificates(certSecret)
            .endTls()
            .build();

        testWeirdUsername(weirdUserName, new KafkaListenerAuthenticationTls(), bridgeSpec, testStorage);
    }

    private void testWeirdUsername(String weirdUserName, KafkaListenerAuthentication auth,
                                   KafkaBridgeSpec spec, TestStorage testStorage) {
        String bridgeProducerName = testStorage.getProducerName() + "-" + TestConstants.BRIDGE;
        String bridgeConsumerName = testStorage.getConsumerName() + "-" + TestConstants.BRIDGE;

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3, 1)
            .editMetadata()
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                        .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                        .withPort(9093)
                        .withType(KafkaListenerType.INTERNAL)
                        .withTls(true)
                        .withAuth(auth)
                        .build()
                    )
                .endKafka()
            .endSpec()
            .build());

        BridgeClients kafkaBridgeClientJob = new BridgeClientsBuilder()
            .withProducerName(bridgeProducerName)
            .withConsumerName(bridgeConsumerName)
            .withBootstrapAddress(KafkaBridgeResources.serviceName(testStorage.getClusterName()))
            .withComponentName(KafkaBridgeResources.componentName(testStorage.getClusterName()))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withPort(TestConstants.HTTP_BRIDGE_DEFAULT_PORT)
            .withNamespaceName(testStorage.getNamespaceName())
            .build();

        // Create topic
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());

        // Create user
        if (auth.getType().equals(TestConstants.TLS_LISTENER_DEFAULT_NAME)) {
            resourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), testStorage.getClusterName(), weirdUserName)
                .editMetadata()
                    .withNamespace(testStorage.getNamespaceName())
                .endMetadata()
                .build());
        } else {
            resourceManager.createResourceWithWait(KafkaUserTemplates.scramShaUser(testStorage.getNamespaceName(), testStorage.getClusterName(), weirdUserName)
                .editMetadata()
                    .withNamespace(testStorage.getNamespaceName())
                .endMetadata()
                .build());
        }

        // Deploy http bridge
        resourceManager.createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(testStorage.getClusterName(), KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()), 1)
            .editMetadata()
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .withNewSpecLike(spec)
                .withBootstrapServers(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
                .withNewHttp(TestConstants.HTTP_BRIDGE_DEFAULT_PORT)
                .withNewConsumer()
                    .addToConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .endConsumer()
            .endSpec()
            .build());

        resourceManager.createResourceWithWait(kafkaBridgeClientJob.consumerStrimziBridge());

        final KafkaClients kafkaClients = ClientUtils.getInstantTlsClientBuilder(testStorage)
            .withUsername(weirdUserName)
            .build();

        if (auth.getType().equals(TestConstants.TLS_LISTENER_DEFAULT_NAME)) {
            // tls producer
            resourceManager.createResourceWithWait(kafkaClients.producerTlsStrimzi(testStorage.getClusterName()));
        } else {
            // scram-sha producer
            resourceManager.createResourceWithWait(kafkaClients.producerScramShaTlsStrimzi(testStorage.getClusterName()));
        }

        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getProducerName(), testStorage.getMessageCount());

        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), bridgeConsumerName, testStorage.getMessageCount());
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
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(suiteTestStorage.getNamespaceName(), suiteTestStorage.getBrokerPoolName(), suiteTestStorage.getClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(suiteTestStorage.getNamespaceName(), suiteTestStorage.getControllerPoolName(), suiteTestStorage.getClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(suiteTestStorage.getClusterName(), 1, 1)
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
                            .withNewKafkaListenerAuthenticationTlsAuth()
                            .endKafkaListenerAuthenticationTlsAuth()
                            .build())
                .endKafka()
            .endSpec()
            .build());

        // Create Kafka user
        KafkaUser tlsUser = KafkaUserTemplates.tlsUser(Environment.TEST_SUITE_NAMESPACE, suiteTestStorage.getClusterName(), suiteTestStorage.getUsername()).build();
        resourceManager.createResourceWithWait(tlsUser);

        // Initialize CertSecretSource with certificate and Secret names for consumer
        CertSecretSource certSecret = new CertSecretSource();
        certSecret.setCertificate("ca.crt");
        certSecret.setSecretName(KafkaResources.clusterCaCertificateSecretName(suiteTestStorage.getClusterName()));

        // Deploy http bridge
        resourceManager.createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(suiteTestStorage.getClusterName(), KafkaResources.tlsBootstrapAddress(suiteTestStorage.getClusterName()), 1)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .editSpec()
                .withNewConsumer()
                    .addToConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .endConsumer()
                .withNewKafkaClientAuthenticationTls()
                    .withNewCertificateAndKey()
                        .withSecretName(suiteTestStorage.getUsername())
                        .withCertificate("user.crt")
                        .withKey("user.key")
                    .endCertificateAndKey()
                .endKafkaClientAuthenticationTls()
                .withNewTls()
                    .withTrustedCertificates(certSecret)
                .endTls()
            .endSpec()
            .build());

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
