/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security;

import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.FileUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.systemtest.annotations.ParallelSuite;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;

import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;

@Tag(REGRESSION)
@Tag(INTERNAL_CLIENTS_USED)
@ParallelSuite
public class OpaIntegrationST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(OpaIntegrationST.class);
    private static final String OPA_SUPERUSER = "arnost";
    private static final String OPA_GOOD_USER = "good-user";
    private static final String OPA_BAD_USER = "bad-user";
    private static final String CLUSTER_NAME = "opa-cluster";

    private final String namespace = testSuiteNamespaceManager.getMapOfAdditionalNamespaces().get(OpaIntegrationST.class.getSimpleName()).stream().findFirst().get();

    @ParallelTest
    void testOpaAuthorization(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final String consumerGroupName = "consumer-group-name-1";
        final String kafkaClientsDeploymentName = clusterName + "-" + Constants.KAFKA_CLIENTS;
        // Deploy client pod with custom certificates and collect messages from internal TLS listener

        KafkaUser goodUser = KafkaUserTemplates.tlsUser(namespace, CLUSTER_NAME, OPA_GOOD_USER).build();
        KafkaUser badUser = KafkaUserTemplates.tlsUser(namespace, CLUSTER_NAME, OPA_BAD_USER).build();

        resourceManager.createResource(extensionContext, goodUser);
        resourceManager.createResource(extensionContext, badUser);
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, kafkaClientsDeploymentName, false, goodUser, badUser)
            .editMetadata()
                .withNamespace(namespace)
            .endMetadata()
            .build());

        final String clientsPodName = kubeClient(namespace).listPodsByPrefixInName(namespace, kafkaClientsDeploymentName).get(0).getMetadata().getName();

        LOGGER.info("Checking KafkaUser {} that is able to send and receive messages to/from topic '{}'", OPA_GOOD_USER, topicName);

        // Setup kafka client
        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(namespace)
            .withClusterName(CLUSTER_NAME)
            .withKafkaUsername(OPA_GOOD_USER)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroupName(consumerGroupName)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withUsingPodName(clientsPodName)
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        internalKafkaClient.produceAndConsumesTlsMessagesUntilBothOperationsAreSuccessful();

        LOGGER.info("Checking KafkaUser {} that is not able to send or receive messages to/from topic '{}'", OPA_BAD_USER, topicName);

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withKafkaUsername(OPA_BAD_USER)
            .build();

        assertThat(internalKafkaClient.sendMessagesTls(), is(-1));
        assertThat(internalKafkaClient.receiveMessagesTls(), is(0));
    }

    @ParallelTest
    void testOpaAuthorizationSuperUser(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final String consumerGroupName = "consumer-group-name-2";
        final String kafkaClientsDeploymentName = clusterName + "-" + Constants.KAFKA_CLIENTS;

        KafkaUser superuser = KafkaUserTemplates.tlsUser(namespace, CLUSTER_NAME, OPA_SUPERUSER).build();

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(CLUSTER_NAME, topicName, namespace).build());
        resourceManager.createResource(extensionContext, superuser);
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, kafkaClientsDeploymentName, false, superuser)
            .editMetadata()
                .withNamespace(namespace)
            .endMetadata()
            .build());

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        String clientsPodName = kubeClient(namespace).listPodsByPrefixInName(namespace, kafkaClientsDeploymentName).get(0).getMetadata().getName();


        LOGGER.info("Checking KafkaUser {} that is able to send and receive messages to/from topic '{}'", OPA_GOOD_USER, topicName);

        // Setup kafka client
        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(namespace)
            .withClusterName(CLUSTER_NAME)
            .withKafkaUsername(OPA_SUPERUSER)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroupName(consumerGroupName)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withUsingPodName(clientsPodName)
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(),
            internalKafkaClient.receiveMessagesTls()
        );
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) throws Exception {
        // Install OPA
        cmdKubeClient().apply(FileUtils.updateNamespaceOfYamlFile(TestUtils.USER_PATH + "/../systemtest/src/test/resources/opa/opa.yaml", namespace));

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(CLUSTER_NAME, 3, 1)
            .editMetadata()
                .withNamespace(namespace)
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withNewKafkaAuthorizationOpa()
                        .withUrl("http://opa:8181/v1/data/kafka/simple/authz/allow")
                        .addToSuperUsers("CN=" + OPA_SUPERUSER)
                    .endKafkaAuthorizationOpa()
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

    @AfterAll
    void teardown() throws IOException {
        // Delete OPA
        cmdKubeClient().delete(FileUtils.updateNamespaceOfYamlFile(TestUtils.USER_PATH + "/../systemtest/src/test/resources/opa/opa.yaml", namespace));
    }
}
