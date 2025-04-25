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
import io.skodjob.testframe.resources.KubeResourceManager;
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
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClients;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClientsBuilder;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
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

import static io.strimzi.systemtest.TestTags.BRIDGE;
import static io.strimzi.systemtest.TestTags.REGRESSION;

@Tag(BRIDGE)
@Tag(REGRESSION)
@SuiteDoc(
    description = @Desc("Test suite for validating Kafka Bridge functionality with TLS and SCRAM-SHA authentication"),
    beforeTestSteps = {
        @Step(value = "Create TestStorage instance.", expected = "TestStorage instance is created."),
        @Step(value = "Create BridgeClients instance.", expected = "BridgeClients instance is created."),
        @Step(value = "Deploy Kafka and KafkaBridge.", expected = "Kafka and KafkaBridge are deployed successfully."),
        @Step(value = "Create Kafka topic.", expected = "Kafka topic is created with the given configuration."),
        @Step(value = "Create Kafka user with SCRAM-SHA authentication.", expected = "Kafka user is created and configured with SCRAM-SHA authentication."),
        @Step(value = "Deploy HTTP bridge.", expected = "HTTP bridge is deployed.")
    },
    afterTestSteps = {
        
    },
    labels = {
        @Label(TestDocsLabels.BRIDGE)
    }
)
class HttpBridgeScramShaST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeScramShaST.class);
    private BridgeClients kafkaBridgeClientJob;
    private TestStorage suiteTestStorage;
    
    @ParallelTest
    @TestDoc(
        description = @Desc("Test ensuring that sending a simple message using TLS and SCRAM-SHA authentication via Kafka Bridge works as expected."),
        steps = {
            @Step(value = "Create TestStorage and BridgeClients objects.", expected = "Instances of TestStorage and BridgeClients are created."),
            @Step(value = "Create topic using the resource manager.", expected = "Topic is created successfully with the specified configuration."),
            @Step(value = "Start producing messages via Kafka Bridge.", expected = "Messages are produced successfully to the topic."),
            @Step(value = "Wait for producer success.", expected = "Producer finishes sending messages without errors."),
            @Step(value = "Create KafkaClients and configure with TLS and SCRAM-SHA.", expected = "Kafka client is configured with appropriate security settings."),
            @Step(value = "Start consuming messages via Kafka client.", expected = "Messages are consumed successfully from the topic."),
            @Step(value = "Wait for consumer success.", expected = "Consumer finishes receiving messages without errors.")
        },
        labels = {
            @Label(TestDocsLabels.BRIDGE)
        }
    )
    void testSendSimpleMessageTlsScramSha() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        final BridgeClients kafkaBridgeClientJb = new BridgeClientsBuilder(kafkaBridgeClientJob)
            .withTopicName(testStorage.getTopicName())
            .withProducerName(testStorage.getProducerName())
            .build();

        // Create topic
        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), suiteTestStorage.getClusterName()).build());

        KubeResourceManager.get().createResourceWithWait(kafkaBridgeClientJb.producerStrimziBridge());
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getProducerName(), testStorage.getMessageCount());

        final KafkaClients kafkaClients = ClientUtils.getInstantTlsClientBuilder(testStorage, KafkaResources.tlsBootstrapAddress(suiteTestStorage.getClusterName()))
            .withUsername(suiteTestStorage.getUsername())
            .build();

        KubeResourceManager.get().createResourceWithWait(kafkaClients.consumerScramShaTlsStrimzi(suiteTestStorage.getClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);
    }

    @ParallelTest
    @TestDoc(
        description = @Desc("Test to check the reception of a simple message via Kafka Bridge using TLS and SCRAM-SHA encryption."),
        steps = {
            @Step(value = "Initialize TestStorage and BridgeClientsBuilder instances.", expected = "Instances are successfully initialized."),
            @Step(value = "Create Kafka topic using ResourceManager.", expected = "Kafka topic is created and available."),
            @Step(value = "Create Bridge consumer using ResourceManager.", expected = "Bridge consumer is successfully created."),
            @Step(value = "Send messages to Kafka using KafkaClients.", expected = "Messages are successfully sent to the Kafka topic."),
            @Step(value = "Wait for clients' success validation.", expected = "Messages are successfully consumed from the Kafka topic.")
        },
        labels = {
            @Label(TestDocsLabels.BRIDGE)
        }
    )
    void testReceiveSimpleMessageTlsScramSha() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        final BridgeClients kafkaBridgeClientJb = new BridgeClientsBuilder(kafkaBridgeClientJob)
            .withTopicName(testStorage.getTopicName())
            .withConsumerName(testStorage.getConsumerName())
            .build();

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), suiteTestStorage.getClusterName()).build());
        KubeResourceManager.get().createResourceWithWait(kafkaBridgeClientJb.consumerStrimziBridge());

        // Send messages to Kafka
        KafkaClients kafkaClients = ClientUtils.getInstantScramShaClientBuilder(testStorage, KafkaResources.tlsBootstrapAddress(suiteTestStorage.getClusterName()))
            .withUsername(suiteTestStorage.getUsername())
            .build();

        KubeResourceManager.get().createResourceWithWait(kafkaClients.producerScramShaTlsStrimzi(suiteTestStorage.getClusterName()));
        ClientUtils.waitForClientsSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getProducerName(), testStorage.getMessageCount());
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
        // Deploy kafka
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(suiteTestStorage.getNamespaceName(), suiteTestStorage.getClusterName(), 1)
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
        KafkaUser scramShaUser = KafkaUserTemplates.scramShaUser(suiteTestStorage.getNamespaceName(), suiteTestStorage.getUsername(), suiteTestStorage.getClusterName()).build();

        KubeResourceManager.get().createResourceWithWait(scramShaUser);

        // Initialize PasswordSecret to set this as PasswordSecret in Bridge spec
        PasswordSecretSource passwordSecret = new PasswordSecretSource();
        passwordSecret.setSecretName(suiteTestStorage.getUsername());
        passwordSecret.setPassword("password");

        // Initialize CertSecretSource with certificate and Secret names for consumer
        CertSecretSource certSecret = new CertSecretSource();
        certSecret.setCertificate("ca.crt");
        certSecret.setSecretName(KafkaResources.clusterCaCertificateSecretName(suiteTestStorage.getClusterName()));

        // Deploy http bridge
        KubeResourceManager.get().createResourceWithWait(
            KafkaBridgeTemplates.kafkaBridge(suiteTestStorage.getNamespaceName(),
                suiteTestStorage.getClusterName(), KafkaResources.tlsBootstrapAddress(suiteTestStorage.getClusterName()), 1)
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
                .endSpec()
                .build()
        );

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
