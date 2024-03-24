/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security;

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
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
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

import java.io.IOException;

import static io.strimzi.systemtest.TestConstants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;

@Tag(REGRESSION)
@Tag(INTERNAL_CLIENTS_USED)
public class OpaIntegrationST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(OpaIntegrationST.class);
    private static final String OPA_SUPERUSER = "arnost";
    private static final String OPA_GOOD_USER = "good-user";
    private static final String OPA_BAD_USER = "bad-user";
    private static final String CLUSTER_NAME = "opa-cluster";

    @ParallelTest
    void testOpaAuthorization() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        KafkaUser goodUser = KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), CLUSTER_NAME, OPA_GOOD_USER).build();
        KafkaUser badUser = KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), CLUSTER_NAME, OPA_BAD_USER).build();

        resourceManager.createResourceWithWait(goodUser);
        resourceManager.createResourceWithWait(badUser);

        LOGGER.info("Checking KafkaUser: {}/{} that is able to send and receive messages to/from Topic: {}/{}", testStorage.getNamespaceName(), OPA_GOOD_USER, testStorage.getNamespaceName(), testStorage.getTopicName());

        KafkaClients kafkaClients = ClientUtils.getInstantTlsClientBuilder(testStorage, KafkaResources.tlsBootstrapAddress(CLUSTER_NAME))
            .withUsername(OPA_GOOD_USER)
            .build();

        resourceManager.createResourceWithWait(kafkaClients.producerTlsStrimzi(CLUSTER_NAME), kafkaClients.consumerTlsStrimzi(CLUSTER_NAME));
        ClientUtils.waitForInstantClientSuccess(testStorage);

        LOGGER.info("Checking KafkaUser: {}/{} that is not able to send or receive messages to/from Topic: {}/{}", testStorage.getNamespaceName(), OPA_BAD_USER, testStorage.getNamespaceName(), testStorage.getTopicName());

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withUsername(OPA_BAD_USER)
            .build();

        resourceManager.createResourceWithWait(kafkaClients.producerTlsStrimzi(CLUSTER_NAME), kafkaClients.consumerTlsStrimzi(CLUSTER_NAME));
        ClientUtils.waitForInstantClientsTimeout(testStorage);
    }

    @ParallelTest
    void testOpaAuthorizationSuperUser() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        KafkaUser superuser = KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), CLUSTER_NAME, OPA_SUPERUSER).build();

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(CLUSTER_NAME, testStorage.getTopicName(), testStorage.getNamespaceName()).build());
        resourceManager.createResourceWithWait(superuser);

        LOGGER.info("Checking KafkaUser: {}/{} that is able to send and receive messages to/from Topic: {}/{}", testStorage.getNamespaceName(), OPA_GOOD_USER, testStorage.getNamespaceName(), testStorage.getTopicName());

        final KafkaClients kafkaClients = ClientUtils.getInstantTlsClientBuilder(testStorage, KafkaResources.tlsBootstrapAddress(CLUSTER_NAME))
            .withUsername(OPA_SUPERUSER)
            .build();

        resourceManager.createResourceWithWait(kafkaClients.producerTlsStrimzi(CLUSTER_NAME), kafkaClients.consumerTlsStrimzi(CLUSTER_NAME));
        ClientUtils.waitForInstantClientSuccess(testStorage);
    }

    @BeforeAll
    void setup() throws Exception {
        this.clusterOperator = this.clusterOperator
            .defaultInstallation()
            .createInstallation()
            .runInstallation();

        // Install OPA
        cmdKubeClient(Environment.TEST_SUITE_NAMESPACE).apply(FileUtils.updateNamespaceOfYamlFile(TestUtils.USER_PATH + "/../systemtest/src/test/resources/opa/opa.yaml", Environment.TEST_SUITE_NAMESPACE));

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(Environment.TEST_SUITE_NAMESPACE, KafkaNodePoolResource.getBrokerPoolName(CLUSTER_NAME), CLUSTER_NAME, 3).build(),
                KafkaNodePoolTemplates.controllerPool(Environment.TEST_SUITE_NAMESPACE, KafkaNodePoolResource.getControllerPoolName(CLUSTER_NAME), CLUSTER_NAME, 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(CLUSTER_NAME, 3, 1)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withNewKafkaAuthorizationOpa()
                        .withUrl("http://opa:8181/v1/data/kafka/simple/authz/allow")
                        .addToSuperUsers("CN=" + OPA_SUPERUSER)
                    .endKafkaAuthorizationOpa()
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

    @AfterAll
    void teardown() throws IOException {
        // Delete OPA
        cmdKubeClient().delete(FileUtils.updateNamespaceOfYamlFile(TestUtils.USER_PATH + "/../systemtest/src/test/resources/opa/opa.yaml", Environment.TEST_SUITE_NAMESPACE));
    }
}
