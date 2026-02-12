/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.custom;

import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
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
import io.strimzi.systemtest.docs.TestDocsLabels;
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
@SuiteDoc(
    description = @Desc("Test suite for verifying custom authorization using ACLs (Access Control Lists) with simple authorization and a TLS listener."),
    beforeTestSteps = {
        @Step(value = "Deploy Kafka cluster configured with custom authorization, TLS listener, and a superuser.", expected = "Kafka cluster is deployed and ready.")
    },
    labels = {
        @Label(value = TestDocsLabels.SECURITY)
    }
)
public class CustomAuthorizerST extends AbstractST {
    static final String ADMIN = "sre-admin";
    private TestStorage sharedTestStorage;
    private static final Logger LOGGER = LogManager.getLogger(CustomAuthorizerST.class);

    @ParallelTest
    @TestDoc(
        description = @Desc("This test case verifies access control lists (ACLs) with simple authorization and a TLS listener."),
        steps = {
            @Step(value = "Create a KafkaUser with ACLs to write to and describe a specific topic.", expected = "KafkaUser authorized to produce to the topic is ready."),
            @Step(value = "Create a second KafkaUser with ACLs to read from and describe the same topic.", expected = "KafkaUser authorized to consume from the topic is ready."),
            @Step(value = "Deploy Kafka clients using the first KafkaUser to produce data to the topic.", expected = "The producer completes successfully, and the consumer times out."),
            @Step(value = "Deploy Kafka clients using the second KafkaUser to consume data from the topic.", expected = "The consumer completes successfully."),
            @Step(value = "Verify that KafkaUser with read-only ACLs cannot produce messages.", expected = "Producer times out.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
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

    @ParallelTest
    @TestDoc(
        description = @Desc("This test verifies that a superuser can produce and consume messages without explicit ACL rules."),
        steps = {
            @Step(value = "Create KafkaUser with TLS certificate CN matching the configured superuser DN.", expected = "The KafkaUser representing the superuser is ready."),
            @Step(value = "Deploy Kafka clients using the superuser KafkaUser.", expected = "The producer and consumer complete successfully.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
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