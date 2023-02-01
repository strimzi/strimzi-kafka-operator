/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka;

import io.strimzi.api.kafka.model.AclOperation;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.annotations.ParallelSuite;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static io.strimzi.systemtest.Constants.KAFKA_SMOKE;

@ParallelSuite
@Tag(KAFKA_SMOKE)
public class KafkaVersionsST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(KafkaVersionsST.class);

    /**
     * Test checking basic functionality for each supported Kafka version.
     * Ensures that for every Kafka version:
     *     - Kafka cluster is deployed without an issue
     *       - with TopicOperator, UserOperator, 3 Zookeeper and Kafka pods
     *     - TopicOperator is working - because of the KafkaTopic creation
     *     - UserOperator is working - because of SCRAM-SHA, ACLs and overall KafkaUser creations
     *     - Sending and receiving messages is working to PLAIN (with SCRAM-SHA) and TLS listeners
     * @param testKafkaVersion TestKafkaVersion added for each iteration of the parametrized test
     * @param extensionContext context in which the current test is being executed
     */
    @ParameterizedTest(name = "Kafka version: {0}.version()")
    @MethodSource("io.strimzi.systemtest.utils.TestKafkaVersion#getSupportedKafkaVersions")
    @KRaftNotSupported("Scram-sha is not supported by KRaft mode and is used in this test case")
    void testKafkaWithVersion(final TestKafkaVersion testKafkaVersion, ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);

        final String kafkaUserRead = testStorage.getUserName() + "-read";
        final String kafkaUserWrite = testStorage.getUserName() + "-write";
        final String kafkaUserReadWriteTls = testStorage.getUserName() + "-read-write";
        final String readConsumerGroup = ClientUtils.generateRandomConsumerGroup();

        LOGGER.info("Deploying Kafka with version: {}", testKafkaVersion.version());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
            .editMetadata()
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .editOrNewSpec()
                .editOrNewKafka()
                    .withVersion(testKafkaVersion.version())
                    .addToConfig("inter.broker.protocol.version", testKafkaVersion.protocolVersion())
                    .addToConfig("log.message.format.version", testKafkaVersion.messageVersion())
                    .withNewKafkaAuthorizationSimple()
                    .endKafkaAuthorizationSimple()
                    .withListeners(
                            new GenericKafkaListenerBuilder()
                                    .withName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
                                    .withPort(9092)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(false)
                                    .withNewKafkaListenerAuthenticationScramSha512Auth()
                                    .endKafkaListenerAuthenticationScramSha512Auth()
                                    .build(),
                            new GenericKafkaListenerBuilder()
                                    .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                                    .withPort(9093)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(true)
                                    .withNewKafkaListenerAuthenticationTlsAuth()
                                    .endKafkaListenerAuthenticationTlsAuth()
                                    .build()
                    )
                .endKafka()
            .endSpec()
            .build()
        );

        KafkaUser writeUser = KafkaUserTemplates.scramShaUser(testStorage.getNamespaceName(), testStorage.getClusterName(), kafkaUserWrite)
            .editSpec()
                .withNewKafkaUserAuthorizationSimple()
                    .addNewAcl()
                        .withNewAclRuleTopicResource()
                            .withName(testStorage.getTopicName())
                        .endAclRuleTopicResource()
                        .withOperations(AclOperation.WRITE, AclOperation.DESCRIBE)
                    .endAcl()
                .endKafkaUserAuthorizationSimple()
            .endSpec()
            .build();

        KafkaUser readUser = KafkaUserTemplates.scramShaUser(testStorage.getNamespaceName(), testStorage.getClusterName(), kafkaUserRead)
            .editSpec()
                .withNewKafkaUserAuthorizationSimple()
                    .addNewAcl()
                        .withNewAclRuleTopicResource()
                            .withName(testStorage.getTopicName())
                        .endAclRuleTopicResource()
                        .withOperations(AclOperation.READ, AclOperation.DESCRIBE)
                    .endAcl()
                    .addNewAcl()
                        .withNewAclRuleGroupResource()
                            .withName(readConsumerGroup)
                        .endAclRuleGroupResource()
                        .withOperations(AclOperation.READ)
                    .endAcl()
                .endKafkaUserAuthorizationSimple()
            .endSpec()
            .build();

        KafkaUser tlsReadWriteUser = KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), testStorage.getClusterName(), kafkaUserReadWriteTls)
                .editSpec()
                    .withNewKafkaUserAuthorizationSimple()
                        .addNewAcl()
                            .withNewAclRuleTopicResource()
                                .withName(testStorage.getTopicName())
                            .endAclRuleTopicResource()
                            .withOperations(AclOperation.WRITE, AclOperation.READ, AclOperation.DESCRIBE)
                        .endAcl()
                        .addNewAcl()
                            .withNewAclRuleGroupResource()
                                .withName(readConsumerGroup)
                            .endAclRuleGroupResource()
                            .withOperations(AclOperation.READ)
                        .endAcl()
                    .endKafkaUserAuthorizationSimple()
                .endSpec()
                .build();

        resourceManager.createResource(extensionContext,
            KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), testStorage.getNamespaceName()).build(),
            readUser,
            writeUser,
            tlsReadWriteUser
        );

        LOGGER.info("Sending and receiving messages via PLAIN -> SCRAM-SHA");

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withNamespaceName(testStorage.getNamespaceName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withMessageCount(testStorage.getMessageCount())
            .withUserName(kafkaUserWrite)
            .withConsumerGroup(readConsumerGroup)
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerScramShaPlainStrimzi());
        ClientUtils.waitForProducerClientSuccess(testStorage);

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
                .withUserName(kafkaUserRead)
                .build();

        resourceManager.createResource(extensionContext, kafkaClients.consumerScramShaPlainStrimzi());
        ClientUtils.waitForConsumerClientSuccess(testStorage);

        LOGGER.info("Sending and receiving messages via TLS");

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withUserName(kafkaUserReadWriteTls)
            .build();

        resourceManager.createResource(extensionContext,
            kafkaClients.producerTlsStrimzi(testStorage.getClusterName()),
            kafkaClients.consumerTlsStrimzi(testStorage.getClusterName())
        );

        ClientUtils.waitForClientsSuccess(testStorage);
    }
}
