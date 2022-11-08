/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.custom;

import io.strimzi.api.kafka.model.AclOperation;
import io.strimzi.api.kafka.model.KafkaAuthorizationSimple;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.annotations.ParallelSuite;
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

import java.rmi.UnexpectedException;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;

@Tag(REGRESSION)
@ParallelSuite
@KRaftNotSupported("Custom Authorizer is not supported by KRaft mode and is used in this test case")
public class CustomAuthorizerST extends AbstractST {
    static final String CLUSTER_NAME = "custom-authorizer";
    static final String ADMIN = "sre-admin";
    private static final Logger LOGGER = LogManager.getLogger(CustomAuthorizerST.class);

    private final String namespace = testSuiteNamespaceManager.getMapOfAdditionalNamespaces().get(CustomAuthorizerST.class.getSimpleName()).stream().findFirst().get();

    @ParallelTest
    @Tag(INTERNAL_CLIENTS_USED)
    void testAclRuleReadAndWrite(ExtensionContext extensionContext) throws UnexpectedException {
        final TestStorage testStorage = new TestStorage(extensionContext, namespace);
        final String kafkaUserWrite = "kafka-user-write";
        final String kafkaUserRead = "kafka-user-read";
        final String consumerGroupName = "consumer-group-name-1";

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(CLUSTER_NAME, testStorage.getTopicName(), namespace).build());

        KafkaUser writeUser = KafkaUserTemplates.tlsUser(namespace, CLUSTER_NAME, kafkaUserWrite)
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

        KafkaUser readUser = KafkaUserTemplates.tlsUser(namespace, CLUSTER_NAME, kafkaUserRead)
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

        resourceManager.createResource(extensionContext, writeUser);
        resourceManager.createResource(extensionContext, readUser);

        LOGGER.info("Checking KafkaUser {} that is able to send messages to topic '{}'", kafkaUserWrite, testStorage.getTopicName());

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withMessageCount(testStorage.getMessageCount())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(CLUSTER_NAME))
            .withTopicName(testStorage.getTopicName())
            .withUserName(kafkaUserWrite)
            .withConsumerGroup(consumerGroupName)
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerTlsStrimzi(CLUSTER_NAME));
        ClientUtils.waitForProducerClientSuccess(testStorage);

        resourceManager.createResource(extensionContext, kafkaClients.consumerTlsStrimzi(CLUSTER_NAME));
        ClientUtils.waitForConsumerClientTimeout(testStorage);

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withUserName(kafkaUserRead)
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.consumerTlsStrimzi(CLUSTER_NAME));
        ClientUtils.waitForConsumerClientSuccess(testStorage);

        LOGGER.info("Checking KafkaUser {} that is not able to send messages to topic '{}'", kafkaUserRead, testStorage.getTopicName());

        resourceManager.createResource(extensionContext, kafkaClients.producerTlsStrimzi(CLUSTER_NAME));
        ClientUtils.waitForProducerClientTimeout(testStorage);
    }

    @ParallelTest
    @Tag(INTERNAL_CLIENTS_USED)
    void testAclWithSuperUser(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, namespace);

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(CLUSTER_NAME, testStorage.getTopicName(), namespace).build());

        KafkaUser adminUser = KafkaUserTemplates.tlsUser(namespace, CLUSTER_NAME, ADMIN)
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

        resourceManager.createResource(extensionContext, adminUser);

        LOGGER.info("Checking kafka super user:{} that is able to send messages to topic:{}", ADMIN, testStorage.getTopicName());

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withMessageCount(testStorage.getMessageCount())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(CLUSTER_NAME))
            .withTopicName(testStorage.getTopicName())
            .withUserName(ADMIN)
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerTlsStrimzi(CLUSTER_NAME));
        ClientUtils.waitForProducerClientSuccess(testStorage);

        LOGGER.info("Checking kafka super user:{} that is able to read messages to topic:{} regardless that " +
            "we configured Acls with only write operation", ADMIN, TOPIC_NAME);

        resourceManager.createResource(extensionContext, kafkaClients.consumerTlsStrimzi(CLUSTER_NAME));
        ClientUtils.waitForConsumerClientSuccess(testStorage);
    }

    @BeforeAll
    public void setup(ExtensionContext extensionContext) {
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(CLUSTER_NAME, 1, 1)
            .editMetadata()
                .withNamespace(namespace)
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withNewKafkaAuthorizationCustom()
                        .withAuthorizerClass(KafkaAuthorizationSimple.AUTHORIZER_CLASS_NAME)
                        .withSupportsAdminApi(true)
                        .withSuperUsers("CN=" + ADMIN)
                    .endKafkaAuthorizationCustom()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
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
