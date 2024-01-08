/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.custom;

import io.strimzi.api.kafka.model.acl.AclOperation;
import io.strimzi.api.kafka.model.kafka.KafkaAuthorizationSimple;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import static io.strimzi.systemtest.TestConstants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.TestConstants.REGRESSION;

@Tag(REGRESSION)
public class CustomAuthorizerST extends AbstractST {
    static final String CLUSTER_NAME = "custom-authorizer";
    static final String ADMIN = "sre-admin";
    private static final Logger LOGGER = LogManager.getLogger(CustomAuthorizerST.class);

    /**
     * @description This test case verifies Access Control Lists with simple authorization and tls listener.
     *
     * @steps
     *  1. - Kafka with simple authorization and tls listener is deployed even before the test itself start
     *     - Kafka with desired authorization and listener is ready
     *  2. - Create first KafkaUser, with ACLs to write and describe specific topic
     *     - KafkaUser authorized to produce into specific topic is ready
     *  3. - Create second KafkaUser, with ACLs to read and describe specific topic
     *     - KafkaUser authorized to consume from specific topic is ready
     *  4. - Deploy Kafka clients using first KafkaUser authorized to produce data into specific topic
     *     - Producer completes successfully whereas consumer timeouts
     *  5. - Deploy Kafka clients using second KafkaUser authorized to consume data into specific topic
     *     - Producer timeouts whereas consumer timeouts
     *
     * @usecase
     *  - custom-authorization
     *  - acls
     *  - kafka-user
     */
    @ParallelTest
    @Tag(INTERNAL_CLIENTS_USED)
    void testAclRuleReadAndWrite(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, Environment.TEST_SUITE_NAMESPACE);
        final String kafkaUserWrite = "kafka-user-write";
        final String kafkaUserRead = "kafka-user-read";
        final String consumerGroupName = "consumer-group-name-1";

        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(CLUSTER_NAME, testStorage.getTopicName(), Environment.TEST_SUITE_NAMESPACE).build());

        KafkaUser writeUser = KafkaUserTemplates.tlsUser(Environment.TEST_SUITE_NAMESPACE, CLUSTER_NAME, kafkaUserWrite)
            .editSpec()
                .withNewKafkaUserAuthorizationSimple()
                    .addNewAcl()
                        .withNewAclRuleTopicResource()
                            .withName(testStorage.getTopicName())
                        .endAclRuleTopicResource()
                        .withOperations(AclOperation.WRITE, AclOperation.DESCRIBE, AclOperation.CREATE) // create is necessary if topic does not exist prior to data production
                    .endAcl()
                .endKafkaUserAuthorizationSimple()
            .endSpec()
            .build();

        KafkaUser readUser = KafkaUserTemplates.tlsUser(Environment.TEST_SUITE_NAMESPACE, CLUSTER_NAME, kafkaUserRead)
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
                            .withName(consumerGroupName)
                        .endAclRuleGroupResource()
                        .withOperations(AclOperation.READ)
                    .endAcl()
                .endKafkaUserAuthorizationSimple()
            .endSpec()
            .build();

        resourceManager.createResourceWithWait(extensionContext, writeUser);
        resourceManager.createResourceWithWait(extensionContext, readUser);

        LOGGER.info("Checking KafkaUser {} that is able to send messages to Topic: {}", kafkaUserWrite, testStorage.getTopicName());

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withMessageCount(testStorage.getMessageCount())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(CLUSTER_NAME))
            .withTopicName(testStorage.getTopicName())
            .withUsername(kafkaUserWrite)
            .withConsumerGroup(consumerGroupName)
            .build();

        resourceManager.createResourceWithWait(extensionContext, kafkaClients.producerTlsStrimzi(CLUSTER_NAME));
        ClientUtils.waitForProducerClientSuccess(testStorage);

        resourceManager.createResourceWithWait(extensionContext, kafkaClients.consumerTlsStrimzi(CLUSTER_NAME));
        ClientUtils.waitForConsumerClientTimeout(testStorage);

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withUsername(kafkaUserRead)
            .build();

        resourceManager.createResourceWithWait(extensionContext, kafkaClients.consumerTlsStrimzi(CLUSTER_NAME));
        ClientUtils.waitForConsumerClientSuccess(testStorage);

        LOGGER.info("Checking KafkaUser: {}/{} that is not able to send messages to Topic: {}", testStorage.getNamespaceName(), kafkaUserRead, testStorage.getTopicName());

        resourceManager.createResourceWithWait(extensionContext, kafkaClients.producerTlsStrimzi(CLUSTER_NAME));
        ClientUtils.waitForProducerClientTimeout(testStorage);
    }

    /**
     * @description This test case verifies Access Control Lists with simple authorization and tls listener.
     *
     * @steps
     *  1. - Kafka with simple authorization and specified superuser is deployed even before the test itself start
     *     - Kafka with desired authorization is ready
     *  2. - Create explicit KafkaUser, with no other properties except necessary metadata and specific name referencing pre-created superuser
     *     - Admin KafkaUser is ready
     *  3. - Deploy Kafka clients using admin KafkaUser
     *     - Producer and consumer complete successfully
     *
     * @usecase
     *  - custom-authorization
     *  - acls
     *  - kafka-user
     */
    @ParallelTest
    @Tag(INTERNAL_CLIENTS_USED)
    void testAclWithSuperUser(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, Environment.TEST_SUITE_NAMESPACE);

        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(CLUSTER_NAME, testStorage.getTopicName(), Environment.TEST_SUITE_NAMESPACE).build());
        resourceManager.createResourceWithWait(extensionContext, KafkaUserTemplates.tlsUser(Environment.TEST_SUITE_NAMESPACE, CLUSTER_NAME, ADMIN).build());

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withMessageCount(testStorage.getMessageCount())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(CLUSTER_NAME))
            .withTopicName(testStorage.getTopicName())
            .withUsername(ADMIN)
            .build();

        LOGGER.info("Checking Kafka Super User: {}/{} is able to produce/consume despite having no explicit rights in KafkaUser", Environment.TEST_SUITE_NAMESPACE, ADMIN);
        resourceManager.createResourceWithWait(extensionContext, kafkaClients.producerTlsStrimzi(CLUSTER_NAME), kafkaClients.consumerTlsStrimzi(CLUSTER_NAME));
        ClientUtils.waitForClientsSuccess(testStorage);
    }

    @BeforeAll
    public void setup(ExtensionContext extensionContext) {
        this.clusterOperator = this.clusterOperator
            .defaultInstallation(extensionContext)
            .createInstallation()
            .runInstallation();

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaPersistent(CLUSTER_NAME, 1, 1)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .editSpec()
                .editKafka()
                    .addToConfig("auto.create.topics.enable", "true")
                    .withNewKafkaAuthorizationCustom()
                        .withAuthorizerClass(Environment.isKRaftModeEnabled() ? KafkaAuthorizationSimple.KRAFT_AUTHORIZER_CLASS_NAME : KafkaAuthorizationSimple.AUTHORIZER_CLASS_NAME)
                        .withSupportsAdminApi(true)
                        .withSuperUsers("CN=" + ADMIN)
                    .endKafkaAuthorizationCustom()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationTls())
                            .build())
                .endKafka()
            .endSpec()
            .build());
    }
}