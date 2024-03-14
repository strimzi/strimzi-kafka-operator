/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka;

import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.acl.AclOperation;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static io.strimzi.systemtest.TestConstants.KAFKA_SMOKE;

@Tag(KAFKA_SMOKE)
public class KafkaVersionsST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(KafkaVersionsST.class);

    /**
     * Test checking basic functionality for each supported Kafka version.
     * Ensures that for every Kafka version:
     *     - Kafka cluster is deployed without an issue
     *       - with Topic Operator, User Operator, 3 Zookeeper and Kafka pods
     *     - Topic Operator is working - because of the KafkaTopic creation
     *     - User Operator is working - because of SCRAM-SHA, ACLs and overall KafkaUser creations
     *     - Sending and receiving messages is working to PLAIN (with SCRAM-SHA) and TLS listeners
     * @param testKafkaVersion TestKafkaVersion added for each iteration of the parametrized test
     */
    @ParameterizedTest(name = "Kafka version: {0}.version()")
    @MethodSource("io.strimzi.systemtest.utils.TestKafkaVersion#getSupportedKafkaVersions")
    void testKafkaWithVersion(final TestKafkaVersion testKafkaVersion) {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        final String kafkaUserRead = testStorage.getUsername() + "-read";
        final String kafkaUserWrite = testStorage.getUsername() + "-write";
        final String kafkaUserReadWriteTls = testStorage.getUsername() + "-read-write";
        final String readConsumerGroup = ClientUtils.generateRandomConsumerGroup();

        LOGGER.info("Deploying Kafka with version: {}", testKafkaVersion.version());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
            .editMetadata()
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .editOrNewSpec()
                .editOrNewKafka()
                    .withVersion(testKafkaVersion.version())
                    .addToConfig("auto.create.topics.enable", "true")
                    .addToConfig("inter.broker.protocol.version", testKafkaVersion.protocolVersion())
                    .addToConfig("log.message.format.version", testKafkaVersion.messageVersion())
                    .withNewKafkaAuthorizationSimple()
                    .endKafkaAuthorizationSimple()
                    .withListeners(
                            new GenericKafkaListenerBuilder()
                                    .withName(TestConstants.PLAIN_LISTENER_DEFAULT_NAME)
                                    .withPort(9092)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(false)
                                    .withNewKafkaListenerAuthenticationScramSha512Auth()
                                    .endKafkaListenerAuthenticationScramSha512Auth()
                                    .build(),
                            new GenericKafkaListenerBuilder()
                                    .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
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
                        // we need CREATE for topic creation in Kafka (auto.create.topics.enable - true)
                        .withOperations(AclOperation.WRITE, AclOperation.DESCRIBE, AclOperation.CREATE)
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

        resourceManager.createResourceWithWait(
            KafkaTopicTemplates.topic(testStorage).build(),
            readUser,
            writeUser,
            tlsReadWriteUser
        );

        LOGGER.info("Sending and receiving messages via PLAIN -> SCRAM-SHA");
        final KafkaClients kafkaClientsPlainScramShaWrite = ClientUtils.getInstantScramShaOverPlainClientBuilder(testStorage)
            .withUsername(kafkaUserWrite)
            .build();

        resourceManager.createResourceWithWait(kafkaClientsPlainScramShaWrite.producerScramShaPlainStrimzi());
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);

        final KafkaClients kafkaClientsPlainScramShaRead = ClientUtils.getInstantScramShaOverPlainClientBuilder(testStorage)
            .withConsumerGroup(readConsumerGroup)
            .withUsername(kafkaUserRead)
            .build();

        resourceManager.createResourceWithWait(kafkaClientsPlainScramShaRead.consumerScramShaPlainStrimzi());
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        LOGGER.info("Sending and receiving messages via TLS");

        final KafkaClients kafkaClientsTlsScramShaRead = ClientUtils.getInstantTlsClientBuilder(testStorage)
            .withConsumerGroup(readConsumerGroup)
            .withUsername(kafkaUserReadWriteTls)
            .build();

        resourceManager.createResourceWithWait(
            kafkaClientsTlsScramShaRead.producerTlsStrimzi(testStorage.getClusterName()),
            kafkaClientsTlsScramShaRead.consumerTlsStrimzi(testStorage.getClusterName())
        );
        ClientUtils.waitForInstantClientSuccess(testStorage);
    }

    @BeforeAll
    void setup() {
        this.clusterOperator = this.clusterOperator
                .defaultInstallation()
                .createInstallation()
                .runInstallation();
    }
}
