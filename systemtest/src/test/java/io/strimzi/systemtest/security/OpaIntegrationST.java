/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security;

import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.utils.FileUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.test.TestUtils;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;

@Tag(REGRESSION)
@Tag(INTERNAL_CLIENTS_USED)
public class OpaIntegrationST extends AbstractST {
    public static final String NAMESPACE = "opa-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(OpaIntegrationST.class);
    private static final String OPA_SUPERUSER = "arnost";
    private static final String OPA_GOOD_USER = "good-user";
    private static final String OPA_BAD_USER = "bad-user";
    private static final String TOPIC_NAME = KafkaTopicUtils.generateRandomNameOfTopic();
    private static String clientsPodName = "";

    @Test
    void testOpaAuthorization() {
        final String consumerGroupName = "consumer-group-name-1";

        LOGGER.info("Checking KafkaUser {} that is able to send and receive messages to/from topic '{}'", OPA_GOOD_USER, TOPIC_NAME);

        // Setup kafka client
        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withKafkaUsername(OPA_GOOD_USER)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroupName(consumerGroupName)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withUsingPodName(clientsPodName)
            .build();

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(),
            internalKafkaClient.receiveMessagesTls()
        );

        LOGGER.info("Checking KafkaUser {} that is not able to send or receive messages to/from topic '{}'", OPA_BAD_USER, TOPIC_NAME);
        internalKafkaClient.setKafkaUsername(OPA_BAD_USER);
        assertThat(internalKafkaClient.sendMessagesTls(), is(-1));
        assertThat(internalKafkaClient.receiveMessagesTls(), is(0));
    }

    @Test
    void testOpaAuthorizationSuperUser() {
        final String consumerGroupName = "consumer-group-name-1";

        LOGGER.info("Checking KafkaUser {} that is able to send and receive messages to/from topic '{}'", OPA_GOOD_USER, TOPIC_NAME);

        // Setup kafka client
        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withKafkaUsername(OPA_SUPERUSER)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroupName(consumerGroupName)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withUsingPodName(clientsPodName)
            .build();

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(),
            internalKafkaClient.receiveMessagesTls()
        );
    }

    @BeforeAll
    void setup() throws Exception {
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE, Constants.CO_OPERATION_TIMEOUT_DEFAULT);

        // Install OPA
        cmdKubeClient().apply(FileUtils.updateNamespaceOfYamlFile(TestUtils.USER_PATH + "/../systemtest/src/test/resources/opa/opa.yaml", NAMESPACE));

        KafkaResource.kafkaEphemeral(CLUSTER_NAME,  3, 1)
            .editSpec()
                .editKafka()
                    .withNewKafkaAuthorizationOpa()
                        .withUrl("http://opa:8181/v1/data/kafka/simple/authz/allow")
                        .addNewSuperUser("CN=" + OPA_SUPERUSER)
                    .endKafkaAuthorizationOpa()
                    .editListeners()
                        .editOrNewTls()
                            .withNewKafkaListenerAuthenticationTlsAuth()
                            .endKafkaListenerAuthenticationTlsAuth()
                        .endTls()
                    .endListeners()
                .endKafka()
            .endSpec()
            .done();

        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME).done();
        KafkaUser goodUser = KafkaUserResource.tlsUser(CLUSTER_NAME, OPA_GOOD_USER).done();
        KafkaUser badUser = KafkaUserResource.tlsUser(CLUSTER_NAME, OPA_BAD_USER).done();
        KafkaUser superuser = KafkaUserResource.tlsUser(CLUSTER_NAME, OPA_SUPERUSER).done();

        final String kafkaClientsDeploymentName = CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS;
        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        KafkaClientsResource.deployKafkaClients(true, kafkaClientsDeploymentName, false, goodUser, badUser, superuser).done();
        clientsPodName = kubeClient().listPodsByPrefixInName(kafkaClientsDeploymentName).get(0).getMetadata().getName();
    }

    @AfterAll
    void teardown() throws IOException {
        // Delete OPA
        cmdKubeClient().delete(FileUtils.updateNamespaceOfYamlFile(TestUtils.USER_PATH + "/../systemtest/src/test/resources/opa/opa.yaml", NAMESPACE));
    }
}
