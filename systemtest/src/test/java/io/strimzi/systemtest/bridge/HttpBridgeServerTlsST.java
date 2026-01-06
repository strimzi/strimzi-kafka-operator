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
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
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

import static io.strimzi.systemtest.TestTags.ACCEPTANCE;
import static io.strimzi.systemtest.TestTags.BRIDGE;
import static io.strimzi.systemtest.TestTags.REGRESSION;

@Tag(REGRESSION)
@Tag(BRIDGE)
@Tag(ACCEPTANCE)
@SuiteDoc(
    description = @Desc("Test suite for verifying TLS support for HTTP Bridge server."),
    beforeTestSteps = {
        @Step(value = "Initialize test storage and context.", expected = "Test storage and context are initialized successfully."),
        @Step(value = "Create Kafka user to generate HTTP Bridge server certificate and key", expected = "Kafka user for generating HTTP Bridge server certificate and key is created."),
        @Step(value = "Deploy Kafka and KafkaBridge configured HTTP Bridge server certificate and key.", expected = "Kafka and KafkaBridge configured with HTTP Bridge server certificate and key are deployed and running."),
        @Step(value = "Create BridgeClients instance.", expected = "BridgeClients instance is created.")
    },
    labels = {
        @Label(TestDocsLabels.BRIDGE)
    }
)
class HttpBridgeServerTlsST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeServerTlsST.class);
    private BridgeClients kafkaBridgeClientJob;
    private TestStorage suiteTestStorage;

    @ParallelTest
    @TestDoc(
        description = @Desc("Test to verify that sending a simple message using TLS works correctly."),
        steps = {
            @Step(value = "Initialize TestStorage and BridgeClients.", expected = "TestStorage and BridgeClients are initialized."),
            @Step(value = "Create Kafka topic using resource manager.", expected = "Kafka topic is successfully created."),
            @Step(value = "Create Kafka Bridge Client job with TLS configuration for producing messages.", expected = "Kafka Bridge Client job with TLS configuration is created and produces messages successfully."),
            @Step(value = "Verify that the producer successfully sends messages.", expected = "Producer successfully sends the expected number of messages."),
            @Step(value = "Create Kafka client for message consumption", expected = "Kafka client consumer is created."),
            @Step(value = "Verify that the consumer successfully receives messages.", expected = "Consumer successfully receives the expected number of messages.")
        },
        labels = {
            @Label(TestDocsLabels.BRIDGE)
        }
    )
    void testSendSimpleMessageTls() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        BridgeClients kafkaBridgeClientJobProduce = new BridgeClientsBuilder(kafkaBridgeClientJob)
            .withTopicName(testStorage.getTopicName())
            .withProducerName(testStorage.getProducerName())
            .build();

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), suiteTestStorage.getClusterName()).build());

        KubeResourceManager.get().createResourceWithWait(kafkaBridgeClientJobProduce.producerTlsStrimziBridge(suiteTestStorage.getClusterName()));
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getProducerName(), testStorage.getMessageCount());

        final KafkaClients kafkaClients = ClientUtils.getInstantPlainClients(testStorage, KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()));
        KubeResourceManager.get().createResourceWithWait(kafkaClients.consumerStrimzi());

        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getMessageCount());
    }

    @ParallelTest
    @TestDoc(
        description = @Desc("Test to verify that a simple message can be received using TLS in a parallel environment."),
        steps = {
            @Step(value = "Initialize TestStorage and BridgeClients.", expected = "TestStorage and BridgeClients are initialized."),
            @Step(value = "Create Kafka topic with provided configurations.", expected = "Kafka topic resource is created and available."),
            @Step(value = "Create Kafka Bridge client job with TLS configuration for consuming messages.", expected = "Kafka Bridge client with TLS configuration is created and started consuming messages."),
            @Step(value = "Create Kafka client for message production.", expected = "Kafka client is configured and initialized."),
            @Step(value = "Verify that producer successfully sent messages.", expected = "Kafka producer client starts successfully and begins sending messages."),
            @Step(value = "Verify message consumption.", expected = "Messages are successfully consumed by the Kafka Bridge consumer.")
        },
        labels = {
            @Label(TestDocsLabels.BRIDGE)
        }
    )
    void testReceiveSimpleMessageTls() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        BridgeClients kafkaBridgeClientJobConsume = new BridgeClientsBuilder(kafkaBridgeClientJob)
            .withTopicName(testStorage.getTopicName())
            .withConsumerName(testStorage.getConsumerName())
            .build();

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), suiteTestStorage.getClusterName()).build());

        // Start receiving messages with bridge
        KubeResourceManager.get().createResourceWithWait(kafkaBridgeClientJobConsume.consumerTlsStrimziBridge(suiteTestStorage.getClusterName()));

        // Send messages to Kafka
        final KafkaClients kafkaClients = ClientUtils.getInstantPlainClients(testStorage, KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()));
        KubeResourceManager.get().createResourceWithWait(kafkaClients.producerStrimzi());

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
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(suiteTestStorage.getNamespaceName(), suiteTestStorage.getClusterName(), 1).build());

        // Create KafkaUser to generate the HTTP Bridge server certificate and key
        // The KafkaBridge serviceName will be used as the CN for the HTTP Bridge server certificate
        KafkaUser tlsUser = KafkaUserTemplates.tlsUser(Environment.TEST_SUITE_NAMESPACE, KafkaBridgeResources.serviceName(suiteTestStorage.getClusterName()), suiteTestStorage.getClusterName()).build();
        KubeResourceManager.get().createResourceWithWait(tlsUser);

        // Deploy http bridge
        KubeResourceManager.get().createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(suiteTestStorage.getNamespaceName(), suiteTestStorage.getClusterName(),
                        KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()), 1)
                .editSpec()
                    .withNewConsumer()
                        .addToConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                    .endConsumer()
                        .withNewHttp()
                            .withPort(8443)
                            .withNewTls()
                                .withNewCertificateAndKey()
                                    .withSecretName(KafkaBridgeResources.serviceName(suiteTestStorage.getClusterName()))
                                    .withCertificate("user.crt")
                                    .withKey("user.key")
                                .endCertificateAndKey()
                            .endTls()
                        .endHttp()
                .endSpec()
                .build());

        kafkaBridgeClientJob = new BridgeClientsBuilder()
            .withBootstrapAddress(KafkaBridgeResources.serviceName(suiteTestStorage.getClusterName()))
            .withComponentName(KafkaBridgeResources.componentName(suiteTestStorage.getClusterName()))
            .withTopicName(suiteTestStorage.getTopicName())
            .withMessageCount(suiteTestStorage.getMessageCount())
            .withPort(8443)
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .build();
    }
}
