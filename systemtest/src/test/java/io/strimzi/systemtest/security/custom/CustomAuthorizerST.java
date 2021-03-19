/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.custom;


import io.strimzi.api.kafka.model.AclOperation;
import io.strimzi.api.kafka.model.KafkaAuthorizationSimple;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@Tag(REGRESSION)
public class CustomAuthorizerST extends AbstractST {
    static final String NAMESPACE = "custom-authorizer-test";
    static final String CLUSTER_NAME = "custom-authorizer";
    static final String ADMIN = "sre-admin";
    private static final Logger LOGGER = LogManager.getLogger(CustomAuthorizerST.class);

    @Test
    @Tag(INTERNAL_CLIENTS_USED)
    void testAclRuleReadAndWrite() {
        final String kafkaUserWrite = "kafka-user-write";
        final String kafkaUserRead = "kafka-user-read";
        final String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
        final int numberOfMessages = 500;
        final String consumerGroupName = "consumer-group-name-1";

        KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(CLUSTER_NAME, topicName).build());

        KafkaUser writeUser = KafkaUserResource.tlsUser(CLUSTER_NAME, kafkaUserWrite)
            .editSpec()
                .withNewKafkaUserAuthorizationSimple()
                    .addNewAcl()
                        .withNewAclRuleTopicResource()
                            .withName(topicName)
                        .endAclRuleTopicResource()
                        .withOperation(AclOperation.WRITE)
                    .endAcl()
                    .addNewAcl()
                        .withNewAclRuleTopicResource()
                            .withName(topicName)
                        .endAclRuleTopicResource()
                        .withOperation(AclOperation.DESCRIBE)
                    .endAcl()
                .endKafkaUserAuthorizationSimple()
            .endSpec()
            .build();

        writeUser = KafkaUserResource.createAndWaitForReadiness(writeUser);

        LOGGER.info("Checking KafkaUser {} that is able to send messages to topic '{}'", kafkaUserWrite, topicName);

        KafkaClientsResource.createAndWaitForReadiness(KafkaClientsResource.deployKafkaClients(true, kafkaClientsName + "-" + Constants.KAFKA_CLIENTS, writeUser).build());
        String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(kafkaClientsName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withKafkaUsername(kafkaUserWrite)
            .withMessageCount(numberOfMessages)
            .withUsingPodName(kafkaClientsPodName)
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        assertThat(internalKafkaClient.sendMessagesTls(), is(numberOfMessages));
        assertThat(internalKafkaClient.receiveMessagesTls(), is(0));

        KafkaUser readUser = KafkaUserResource.tlsUser(CLUSTER_NAME, kafkaUserRead)
            .editSpec()
                .withNewKafkaUserAuthorizationSimple()
                    .addNewAcl()
                        .withNewAclRuleTopicResource()
                            .withName(topicName)
                        .endAclRuleTopicResource()
                        .withOperation(AclOperation.READ)
                    .endAcl()
                    .addNewAcl()
                        .withNewAclRuleGroupResource()
                            .withName(consumerGroupName)
                        .endAclRuleGroupResource()
                        .withOperation(AclOperation.READ)
                    .endAcl()
                    .addNewAcl()
                        .withNewAclRuleTopicResource()
                            .withName(topicName)
                        .endAclRuleTopicResource()
                        .withOperation(AclOperation.DESCRIBE)
                    .endAcl()
                .endKafkaUserAuthorizationSimple()
            .endSpec()
            .build();

        readUser = KafkaUserResource.createAndWaitForReadiness(readUser);
        KafkaClientsResource.createAndWaitForReadiness(KafkaClientsResource.deployKafkaClients(true, kafkaClientsName + "-" + Constants.KAFKA_CLIENTS, readUser).build());
        kafkaClientsPodName = kubeClient().listPodsByPrefixInName(kafkaClientsName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient newInternalKafkaClient = internalKafkaClient.toBuilder()
            .withKafkaUsername(kafkaUserRead)
            .withConsumerGroupName(consumerGroupName)
            .withUsingPodName(kafkaClientsPodName)
            .build();

        assertThat(newInternalKafkaClient.receiveMessagesTls(), is(numberOfMessages));

        LOGGER.info("Checking KafkaUser {} that is not able to send messages to topic '{}'", kafkaUserRead, topicName);
        assertThat(newInternalKafkaClient.sendMessagesTls(), is(-1));
    }

    @Test
    @Tag(INTERNAL_CLIENTS_USED)
    void testAclWithSuperUser() {
        KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME).build());

        KafkaUser adminUser = KafkaUserResource.tlsUser(CLUSTER_NAME, ADMIN)
            .editSpec()
                .withNewKafkaUserAuthorizationSimple()
                    .addNewAcl()
                        .withNewAclRuleTopicResource()
                            .withName(TOPIC_NAME)
                        .endAclRuleTopicResource()
                        .withOperation(AclOperation.WRITE)
                    .endAcl()
                    .addNewAcl()
                        .withNewAclRuleTopicResource()
                            .withName(TOPIC_NAME)
                        .endAclRuleTopicResource()
                        .withOperation(AclOperation.DESCRIBE)
                    .endAcl()
                .endKafkaUserAuthorizationSimple()
            .endSpec()
            .build();

        adminUser = KafkaUserResource.createAndWaitForReadiness(adminUser);
        KafkaClientsResource.createAndWaitForReadiness(KafkaClientsResource.deployKafkaClients(true, kafkaClientsName + "-" + Constants.KAFKA_CLIENTS, adminUser).build());
        String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(kafkaClientsName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        LOGGER.info("Checking kafka super user:{} that is able to send messages to topic:{}", ADMIN, TOPIC_NAME);

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withKafkaUsername(ADMIN)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .withUsingPodName(kafkaClientsPodName)
            .build();

        assertThat(internalKafkaClient.sendMessagesTls(), is(MESSAGE_COUNT));

        LOGGER.info("Checking kafka super user:{} that is able to read messages to topic:{} regardless that " +
                "we configured Acls with only write operation", ADMIN, TOPIC_NAME);

        assertThat(internalKafkaClient.receiveMessagesTls(), is(MESSAGE_COUNT));
    }

    @BeforeAll
    public void setup() {
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE);

        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(CLUSTER_NAME,  1, 1)
            .editSpec()
                .editKafka()
                    .withNewKafkaAuthorizationCustom()
                        .withAuthorizerClass(KafkaAuthorizationSimple.AUTHORIZER_CLASS_NAME)
                        .withSuperUsers("CN=" + ADMIN)
                    .endKafkaAuthorizationCustom()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationTls())
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());
    }
}
