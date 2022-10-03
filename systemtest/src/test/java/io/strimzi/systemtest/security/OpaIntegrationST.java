/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.ParallelSuite;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.FileUtils;
import io.strimzi.test.TestUtils;
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
        final TestStorage testStorage = new TestStorage(extensionContext, namespace);

        KafkaUser goodUser = KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), CLUSTER_NAME, OPA_GOOD_USER).build();
        KafkaUser badUser = KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), CLUSTER_NAME, OPA_BAD_USER).build();

        resourceManager.createResource(extensionContext, goodUser);
        resourceManager.createResource(extensionContext, badUser);

        LOGGER.info("Checking KafkaUser {} that is able to send and receive messages to/from topic '{}'", OPA_GOOD_USER, testStorage.getTopicName());

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withMessageCount(testStorage.getMessageCount())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(CLUSTER_NAME))
            .withTopicName(testStorage.getTopicName())
            .withUserName(OPA_GOOD_USER)
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerTlsStrimzi(CLUSTER_NAME), kafkaClients.consumerTlsStrimzi(CLUSTER_NAME));
        ClientUtils.waitForClientsSuccess(testStorage);

        LOGGER.info("Checking KafkaUser {} that is not able to send or receive messages to/from topic '{}'", OPA_BAD_USER, testStorage.getTopicName());

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withUserName(OPA_BAD_USER)
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerTlsStrimzi(CLUSTER_NAME), kafkaClients.consumerTlsStrimzi(CLUSTER_NAME));
        ClientUtils.waitForClientsTimeout(testStorage);
    }

    @ParallelTest
    void testOpaAuthorizationSuperUser(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, namespace);

        KafkaUser superuser = KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), CLUSTER_NAME, OPA_SUPERUSER).build();

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(CLUSTER_NAME, testStorage.getTopicName(), testStorage.getNamespaceName()).build());
        resourceManager.createResource(extensionContext, superuser);

        LOGGER.info("Checking KafkaUser {} that is able to send and receive messages to/from topic '{}'", OPA_GOOD_USER, testStorage.getTopicName());

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withMessageCount(testStorage.getMessageCount())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(CLUSTER_NAME))
            .withTopicName(testStorage.getTopicName())
            .withUserName(OPA_SUPERUSER)
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerTlsStrimzi(CLUSTER_NAME), kafkaClients.consumerTlsStrimzi(CLUSTER_NAME));
        ClientUtils.waitForClientsSuccess(testStorage);
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
