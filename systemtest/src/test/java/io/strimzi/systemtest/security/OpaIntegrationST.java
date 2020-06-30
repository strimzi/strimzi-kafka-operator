package io.strimzi.systemtest.security;

import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.systemtest.BaseST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.utils.FileUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.test.WaitException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(REGRESSION)
public class OpaIntegrationST extends BaseST {
    public static final String NAMESPACE = "opa-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(OpaIntegrationST.class);
    private static final String OPA_SUPERUSER_1 = "arnost";
    private static final String OPA_SUPERUSER_2 = "franta";
    private static final String OPA_GOOD_USER = "good-user";
    private static final String OPA_BAD_USER = "bad-user";
    private static final String TOPIC_NAME = KafkaTopicUtils.generateRandomNameOfTopic();
    private static String CLIENTS_POD_NAME = "";

    @Test
    @Tag(INTERNAL_CLIENTS_USED)
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
            .withUsingPodName(CLIENTS_POD_NAME)
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
    @Tag(INTERNAL_CLIENTS_USED)
    void testOpaAuthorizationSuperUser() {
        final String consumerGroupName = "consumer-group-name-1";

        LOGGER.info("Checking KafkaUser {} that is able to send and receive messages to/from topic '{}'", OPA_GOOD_USER, TOPIC_NAME);

        // Setup kafka client
        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withKafkaUsername(OPA_SUPERUSER_1)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroupName(consumerGroupName)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withUsingPodName(CLIENTS_POD_NAME)
            .build();

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(),
            internalKafkaClient.receiveMessagesTls()
        );

        internalKafkaClient.setKafkaUsername(OPA_SUPERUSER_2);

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
        cmdKubeClient().apply(FileUtils.updateNamespaceOfYamlFile("../systemtest/src/test/resources/opa/opa.yaml", NAMESPACE));

        KafkaResource.kafkaEphemeral(CLUSTER_NAME,  3, 1)
            .editMetadata()
                .addToLabels("type", "kafka-ephemeral")
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withNewKafkaAuthorizationOpa()
                        .withUrl("http://opa:8181/v1/data/kafka/simple/authz/allow")
                        .addNewSuperUser("CN=" + OPA_SUPERUSER_1)
                        .addNewSuperUser(OPA_SUPERUSER_2)
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
        KafkaUser superuser1 = KafkaUserResource.tlsUser(CLUSTER_NAME, OPA_SUPERUSER_1).done();
        KafkaUser superuser2 = KafkaUserResource.tlsUser(CLUSTER_NAME, OPA_SUPERUSER_2).done();

        final String kafkaClientsDeploymentName = CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS;
        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        KafkaClientsResource.deployKafkaClients(true, kafkaClientsDeploymentName, false, goodUser, badUser, superuser1, superuser2).done();
        CLIENTS_POD_NAME = kubeClient().listPodsByPrefixInName(kafkaClientsDeploymentName).get(0).getMetadata().getName();
    }

    // We don't need to recreate environment, because same resources are used accross whole class
    @Override
    protected void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
        LOGGER.info("Skip env recreation after failed tests!");
    }

    @AfterAll
    void teardown() throws IOException {
        // Delete OPA
        cmdKubeClient().delete(FileUtils.updateNamespaceOfYamlFile("../systemtest/src/test/resources/opa/opa.yaml", NAMESPACE));
    }
}
