/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
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
import io.strimzi.systemtest.TestTags;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.kafkaclients.ClientsAuthentication;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.testclients.clients.http.HttpProducerConsumer;
import io.strimzi.testclients.clients.http.HttpProducerConsumerBuilder;
import io.strimzi.testclients.clients.kafka.KafkaProducerConsumer;
import io.strimzi.testclients.clients.kafka.KafkaProducerConsumerBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import static io.strimzi.systemtest.TestTags.ACCEPTANCE;
import static io.strimzi.systemtest.TestTags.BRIDGE;
import static io.strimzi.systemtest.TestTags.REGRESSION;

@Tag(REGRESSION)
@Tag(BRIDGE)
@Tag(ACCEPTANCE)
@SuiteDoc(
    description = @Desc("Test suite for verifying TLS functionalities in the HTTP Bridge."),
    beforeTestSteps = {
        @Step(value = "Initialize test storage and context.", expected = "Test storage and context are initialized successfully."),
        @Step(value = "Deploy Kafka and KafkaBridge.", expected = "Kafka and KafkaBridge are deployed and running."),
        @Step(value = "Create Kafka user with TLS configuration.", expected = "Kafka user with TLS configuration is created."),
        @Step(value = "Deploy HTTP bridge with TLS configuration.", expected = "HTTP bridge is deployed with TLS configuration.")
    },
    labels = {
        @Label(TestDocsLabels.BRIDGE)
    }
)
class HttpBridgeTlsST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeTlsST.class);

    private HttpProducerConsumerBuilder httpProducerConsumerBuilder;
    private KafkaProducerConsumerBuilder kafkaProducerConsumerBuilder;
    private TestStorage suiteTestStorage;

    @ParallelTest
    @TestDoc(
        description = @Desc("Test to verify that sending a simple message using TLS works correctly."),
        steps = {
            @Step(value = "Initialize TestStorage and BridgeClients with TLS configuration.", expected = "TestStorage and BridgeClients are initialized with TLS configuration."),
            @Step(value = "Create Kafka topic using resource manager.", expected = "Kafka topic is successfully created."),
            @Step(value = "Create Kafka Bridge Client job for producing messages.", expected = "Kafka Bridge Client job is created and produces messages successfully."),
            @Step(value = "Verify that the producer successfully sends messages.", expected = "Producer successfully sends the expected number of messages."),
            @Step(value = "Create Kafka client consumer with TLS configuration.", expected = "Kafka client consumer is created with TLS configuration."),
            @Step(value = "Verify that the consumer successfully receives messages.", expected = "Consumer successfully receives the expected number of messages.")
        },
        labels = {
            @Label(TestDocsLabels.BRIDGE)
        }
    )
    void testSendSimpleMessageTls() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        final HttpProducerConsumer httpProducerConsumer = httpProducerConsumerBuilder
            .withTopicName(testStorage.getTopicName())
            .withProducerName(testStorage.getProducerName())
            .build();

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), suiteTestStorage.getClusterName()).build());

        KubeResourceManager.get().createResourceWithWait(httpProducerConsumer.getProducer().getJob());
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getProducerName(), testStorage.getMessageCount());

        final KafkaProducerConsumer kafkaProducerConsumer = kafkaProducerConsumerBuilder
            .withConsumerName(testStorage.getConsumerName())
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .withTopicName(testStorage.getTopicName())
            .build();

        KubeResourceManager.get().createResourceWithWait(kafkaProducerConsumer.getConsumer().getJob());
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getMessageCount());
    }

    @ParallelTest
    @TestDoc(
        description = @Desc("Test to verify that a simple message can be received using TLS in a parallel environment."),
        steps = {
            @Step(value = "Initialize the test storage instance.", expected = "TestStorage object is instantiated with the test context."),
            @Step(value = "Configure Kafka Bridge client for consumption.", expected = "Kafka Bridge client is configured with topic and consumer names."),
            @Step(value = "Create Kafka topic with provided configurations.", expected = "Kafka topic resource is created and available."),
            @Step(value = "Deploy the Kafka Bridge consumer.", expected = "Kafka Bridge consumer starts successfully and is ready to consume messages."),
            @Step(value = "Initialize TLS Kafka client for message production.", expected = "TLS Kafka client is configured and initialized."),
            @Step(value = "Deploy the Kafka producer TLS client.", expected = "TLS Kafka producer client starts successfully and begins sending messages."),
            @Step(value = "Verify message consumption.", expected = "Messages are successfully consumed by the Kafka Bridge consumer.")
        },
        labels = {
            @Label(TestDocsLabels.BRIDGE)
        }
    )
    void testReceiveSimpleMessageTls() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        final HttpProducerConsumer httpProducerConsumer = httpProducerConsumerBuilder
            .withTopicName(testStorage.getTopicName())
            .withConsumerName(testStorage.getConsumerName())
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), suiteTestStorage.getClusterName()).build());

        KubeResourceManager.get().createResourceWithWait(httpProducerConsumer.getConsumer().getJob());

        // Send messages to Kafka
        final KafkaProducerConsumer kafkaProducerConsumer = kafkaProducerConsumerBuilder
            .withProducerName(testStorage.getProducerName())
            .withTopicName(testStorage.getTopicName())
            .build();

        KubeResourceManager.get().createResourceWithWait(kafkaProducerConsumer.getProducer().getJob());
        ClientUtils.waitForClientsSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getProducerName(), testStorage.getMessageCount());
    }

    @ParallelTest
    void testTlsScramShaAuthWithWeirdUsername() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        // Create weird named user with . and more than 64 chars -> SCRAM-SHA
        final String weirdUserName = "jjglmahyijoambryleyxjjglmahy.ijoambryleyxjjglmahyijoambryleyxasd.asdasidioiqweioqiweooioqieioqieoqieooi";

        // Initialize PasswordSecret to set this as PasswordSecret in Bridge spec
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
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
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
        String bridgeProducerName = testStorage.getProducerName() + "-" + TestTags.BRIDGE;
        String bridgeConsumerName = testStorage.getConsumerName() + "-" + TestTags.BRIDGE;

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
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

        HttpProducerConsumer httpProducerConsumer = new HttpProducerConsumerBuilder()
            .withProducerName(bridgeProducerName)
            .withConsumerName(bridgeConsumerName)
            .withHostname(KafkaBridgeResources.serviceName(testStorage.getClusterName()))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withPort(TestConstants.HTTP_BRIDGE_DEFAULT_PORT)
            .withNamespaceName(testStorage.getNamespaceName())
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        // Create topic
        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());

        // Create user
        if (auth.getType().equals(TestConstants.TLS_LISTENER_DEFAULT_NAME)) {
            KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), weirdUserName, testStorage.getClusterName()).build());
        } else {
            KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.scramShaUser(testStorage.getNamespaceName(), weirdUserName, testStorage.getClusterName()).build());
        }

        // Deploy http bridge
        KubeResourceManager.get().createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()), 1)
            .withNewSpecLike(spec)
                .withBootstrapServers(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
                .withNewHttp(TestConstants.HTTP_BRIDGE_DEFAULT_PORT)
                .withNewConsumer()
                    .addToConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .endConsumer()
            .endSpec()
            .build());

        KubeResourceManager.get().createResourceWithWait(httpProducerConsumer.getConsumer().getJob());

        final KafkaProducerConsumerBuilder producerConsumerBuilder = new KafkaProducerConsumerBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withMessageCount(testStorage.getMessageCount())
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()));

        if (auth.getType().equals(TestConstants.TLS_LISTENER_DEFAULT_NAME)) {
            // tls producer
            producerConsumerBuilder
                .withAuthentication(ClientsAuthentication.configureTls(testStorage.getClusterName(), weirdUserName));
        } else {
            // scram-sha producer
            producerConsumerBuilder
                .withAuthentication(ClientsAuthentication.configureTlsScramSha(testStorage.getNamespaceName(), weirdUserName, testStorage.getClusterName()));
        }

        KubeResourceManager.get().createResourceWithWait(producerConsumerBuilder.build().getProducer().getJob());

        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getProducerName(), testStorage.getMessageCount());

        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), bridgeConsumerName, testStorage.getMessageCount());
    }

    @BeforeAll
    void setUp() {
        suiteTestStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        SetupClusterOperator
            .getInstance()
            .withDefaultConfiguration()
            .install();

        LOGGER.info("Deploying Kafka and KafkaBridge before tests");

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(suiteTestStorage.getNamespaceName(), suiteTestStorage.getBrokerPoolName(), suiteTestStorage.getClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(suiteTestStorage.getNamespaceName(), suiteTestStorage.getControllerPoolName(), suiteTestStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(Environment.TEST_SUITE_NAMESPACE, suiteTestStorage.getClusterName(), 1)
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
        KafkaUser tlsUser = KafkaUserTemplates.tlsUser(Environment.TEST_SUITE_NAMESPACE, suiteTestStorage.getUsername(), suiteTestStorage.getClusterName()).build();
        KubeResourceManager.get().createResourceWithWait(tlsUser);

        // Initialize CertSecretSource with certificate and Secret names for consumer
        CertSecretSource certSecret = new CertSecretSource();
        certSecret.setCertificate("ca.crt");
        certSecret.setSecretName(KafkaResources.clusterCaCertificateSecretName(suiteTestStorage.getClusterName()));

        // Deploy http bridge
        KubeResourceManager.get().createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(Environment.TEST_SUITE_NAMESPACE, suiteTestStorage.getClusterName(), KafkaResources.tlsBootstrapAddress(suiteTestStorage.getClusterName()), 1)
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

        kafkaProducerConsumerBuilder = new KafkaProducerConsumerBuilder()
            .withNamespaceName(suiteTestStorage.getNamespaceName())
            .withMessageCount(suiteTestStorage.getMessageCount())
            .withTopicName(suiteTestStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(suiteTestStorage.getClusterName()))
            .withAuthentication(ClientsAuthentication.configureTls(suiteTestStorage.getClusterName(), suiteTestStorage.getUsername()));

        httpProducerConsumerBuilder = new HttpProducerConsumerBuilder()
            .withHostname(KafkaBridgeResources.serviceName(suiteTestStorage.getClusterName()))
            .withTopicName(suiteTestStorage.getTopicName())
            .withMessageCount(suiteTestStorage.getMessageCount())
            .withPort(TestConstants.HTTP_BRIDGE_DEFAULT_PORT)
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE);
    }
}
