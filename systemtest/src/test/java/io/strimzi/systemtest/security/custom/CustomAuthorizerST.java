/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.custom;

import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.kafka.KafkaAuthorizationSimple;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.acl.AclOperation;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import static io.strimzi.systemtest.TestTags.REGRESSION;

@Tag(REGRESSION)
public class CustomAuthorizerST extends AbstractST {
    static final String ADMIN = "sre-admin";
    private TestStorage sharedTestStorage;
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
    void testAclRuleReadAndWrite() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final String kafkaUserWrite = "kafka-user-write";
        final String kafkaUserRead = "kafka-user-read";
        final String consumerGroupName = "consumer-group-name-1";

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), sharedTestStorage.getClusterName()).build());

        KafkaUser writeUser = KafkaUserTemplates.tlsUser(Environment.TEST_SUITE_NAMESPACE, kafkaUserWrite, sharedTestStorage.getClusterName())
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

        KafkaUser readUser = KafkaUserTemplates.tlsUser(Environment.TEST_SUITE_NAMESPACE, kafkaUserRead, sharedTestStorage.getClusterName())
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

        KubeResourceManager.get().createResourceWithWait(writeUser);
        KubeResourceManager.get().createResourceWithWait(readUser);

        LOGGER.info("Checking KafkaUser {} that is able to send messages to Topic: {}", kafkaUserWrite, testStorage.getTopicName());

        KafkaClients kafkaClients = ClientUtils.getInstantTlsClientBuilder(testStorage, KafkaResources.tlsBootstrapAddress(sharedTestStorage.getClusterName()))
            .withUsername(kafkaUserWrite)
            .withConsumerGroup(consumerGroupName)
            .build();

        KubeResourceManager.get().createResourceWithWait(kafkaClients.producerTlsStrimzi(sharedTestStorage.getClusterName()));
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);

        KubeResourceManager.get().createResourceWithWait(kafkaClients.consumerTlsStrimzi(sharedTestStorage.getClusterName()));
        ClientUtils.waitForInstantConsumerClientTimeout(testStorage);

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withUsername(kafkaUserRead)
            .build();

        KubeResourceManager.get().createResourceWithWait(kafkaClients.consumerTlsStrimzi(sharedTestStorage.getClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        LOGGER.info("Checking KafkaUser: {}/{} that is not able to send messages to Topic: {}", testStorage.getNamespaceName(), kafkaUserRead, testStorage.getTopicName());

        KubeResourceManager.get().createResourceWithWait(kafkaClients.producerTlsStrimzi(sharedTestStorage.getClusterName()));
        ClientUtils.waitForInstantProducerClientTimeout(testStorage);
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
    void testAclWithSuperUser() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), sharedTestStorage.getClusterName()).build());
        KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.tlsUser(Environment.TEST_SUITE_NAMESPACE, ADMIN, sharedTestStorage.getClusterName()).build());

        final KafkaClients kafkaClients = ClientUtils.getInstantTlsClientBuilder(testStorage, KafkaResources.tlsBootstrapAddress(sharedTestStorage.getClusterName()))
            .withUsername(ADMIN)
            .build();

        LOGGER.info("Checking Kafka Super User: {}/{} is able to produce/consume despite having no explicit rights in KafkaUser", Environment.TEST_SUITE_NAMESPACE, ADMIN);
        KubeResourceManager.get().createResourceWithWait(kafkaClients.producerTlsStrimzi(sharedTestStorage.getClusterName()), kafkaClients.consumerTlsStrimzi(sharedTestStorage.getClusterName()));
        ClientUtils.waitForInstantClientSuccess(testStorage);
    }

    @BeforeAll
    public void setup() {
        sharedTestStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        SetupClusterOperator
            .getInstance()
            .withDefaultConfiguration()
            .install();

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(sharedTestStorage.getNamespaceName(), sharedTestStorage.getBrokerPoolName(), sharedTestStorage.getClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(sharedTestStorage.getNamespaceName(), sharedTestStorage.getControllerPoolName(), sharedTestStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(sharedTestStorage.getNamespaceName(), sharedTestStorage.getClusterName(), 1)
            .editSpec()
                .editKafka()
                    .addToConfig("auto.create.topics.enable", "true")
                    .withNewKafkaAuthorizationCustom()
                        .withAuthorizerClass(KafkaAuthorizationSimple.KRAFT_AUTHORIZER_CLASS_NAME)
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