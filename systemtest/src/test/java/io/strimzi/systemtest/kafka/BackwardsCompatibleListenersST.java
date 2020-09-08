/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.listener.KafkaListeners;
import io.strimzi.api.kafka.model.listener.KafkaListenersBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.ArrayOrObjectKafkaListeners;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.kafkaclients.externalClients.BasicExternalKafkaClient;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.ServiceUtils;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.LOADBALANCER_SUPPORTED;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

@Tag(REGRESSION)
public class BackwardsCompatibleListenersST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(BackwardsCompatibleListenersST.class);
    public static final String NAMESPACE = "kafka-listeners-bc-cluster-test";

    /**
     * Test sending messages over tls transport using mutual tls auth
     */
    @Test
    @Tag(INTERNAL_CLIENTS_USED)
    void testSendMessagesTlsAuthenticated() {
        String kafkaUsername = KafkaUserUtils.generateRandomNameOfKafkaUser();
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        KafkaListeners listeners = new KafkaListenersBuilder()
                .withNewTls()
                    .withAuth(new KafkaListenerAuthenticationTls())
                .endTls()
                .build();

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3)
                .editSpec()
                    .editKafka()
                        .withListeners(new ArrayOrObjectKafkaListeners(null, listeners))
                    .endKafka()
                .endSpec()
                .done();

        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();
        KafkaUser user = KafkaUserResource.tlsUser(CLUSTER_NAME, kafkaUsername).done();

        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, user).done();

        final String kafkaClientsPodName =
            ResourceManager.kubeClient().listPodsByPrefixInName("my-cluster" + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withKafkaUsername(kafkaUsername)
            .withMessageCount(MESSAGE_COUNT)
            .build();

        // Check brokers availability
        LOGGER.info("Checking produced and consumed messages to pod: {}", kafkaClientsPodName);

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(),
            internalKafkaClient.receiveMessagesTls()
        );
    }

    /**
     * Test sending messages over plain transport using scram sha auth
     */
    @Test
    @Tag(INTERNAL_CLIENTS_USED)
    void testSendMessagesPlainScramSha() {
        String kafkaUsername = KafkaUserUtils.generateRandomNameOfKafkaUser();
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        KafkaListeners listeners = new KafkaListenersBuilder()
                .withNewPlain()
                    .withAuth(new KafkaListenerAuthenticationScramSha512())
                .endPlain()
                .build();

        // Use a Kafka with plain listener disabled
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3)
                .editSpec()
                    .editKafka()
                        .withListeners(new ArrayOrObjectKafkaListeners(null, listeners))
                    .endKafka()
                .endSpec()
                .done();

        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();
        KafkaUser kafkaUser = KafkaUserResource.scramShaUser(CLUSTER_NAME, kafkaUsername).done();

        KafkaClientsResource.deployKafkaClients(false, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, kafkaUser).done();

        final String kafkaClientsPodName =
            ResourceManager.kubeClient().listPodsByPrefixInName("my-cluster" + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withKafkaUsername(kafkaUsername)
            .withMessageCount(MESSAGE_COUNT)
            .build();

        // Check brokers availability
        LOGGER.info("Checking produced and consumed messages to pod: {}", kafkaClientsPodName);

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );
    }

    @Test
    @Tag(ACCEPTANCE)
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    void testNodePortTls() {
        String kafkaUsername = KafkaUserUtils.generateRandomNameOfKafkaUser();
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        KafkaListeners listeners = new KafkaListenersBuilder()
                .withNewKafkaListenerExternalNodePort()
                    .withAuth(new KafkaListenerAuthenticationTls())
                .endKafkaListenerExternalNodePort()
                .build();

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1)
            .editSpec()
                .editKafka()
                    .withListeners(new ArrayOrObjectKafkaListeners(null, listeners))
                .endKafka()
            .endSpec()
            .done();

        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();
        KafkaUserResource.tlsUser(CLUSTER_NAME, kafkaUsername).done();

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
                .withTopicName(topicName)
                .withNamespaceName(NAMESPACE)
                .withClusterName(CLUSTER_NAME)
                .withMessageCount(MESSAGE_COUNT)
                .withKafkaUsername(kafkaUsername)
                .withSecurityProtocol(SecurityProtocol.SSL)
                .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesTls(),
            basicExternalKafkaClient.receiveMessagesTls()
        );
    }

    @Test
    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    void testLoadBalancerTls() {
        String kafkaUsername = KafkaUserUtils.generateRandomNameOfKafkaUser();
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        KafkaListeners listeners = new KafkaListenersBuilder()
                .withNewKafkaListenerExternalLoadBalancer()
                    .withAuth(new KafkaListenerAuthenticationTls())
                .endKafkaListenerExternalLoadBalancer()
                .build();

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3)
            .editSpec()
                .editKafka()
                    .withListeners(new ArrayOrObjectKafkaListeners(null, listeners))
                .endKafka()
            .endSpec()
            .done();

        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();
        KafkaUserResource.tlsUser(CLUSTER_NAME, kafkaUsername).done();

        ServiceUtils.waitUntilAddressIsReachable(kubeClient().getService(KafkaResources.externalBootstrapServiceName(CLUSTER_NAME)).getStatus().getLoadBalancer().getIngress().get(0).getHostname());

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
                .withTopicName(topicName)
                .withNamespaceName(NAMESPACE)
                .withClusterName(CLUSTER_NAME)
                .withMessageCount(MESSAGE_COUNT)
                .withKafkaUsername(kafkaUsername)
                .withSecurityProtocol(SecurityProtocol.SSL)
                .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesTls(),
            basicExternalKafkaClient.receiveMessagesTls()
        );
    }

    @Test
    @OpenShiftOnly
    @Tag(EXTERNAL_CLIENTS_USED)
    void testRouteTls() {
        String kafkaUsername = KafkaUserUtils.generateRandomNameOfKafkaUser();
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        KafkaListeners listeners = new KafkaListenersBuilder()
                .withNewKafkaListenerExternalRoute()
                    .withAuth(new KafkaListenerAuthenticationTls())
                .endKafkaListenerExternalRoute()
                .build();

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3)
                .editSpec()
                    .editKafka()
                        .withListeners(new ArrayOrObjectKafkaListeners(null, listeners))
                    .endKafka()
                .endSpec()
                .done();

        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();
        KafkaUserResource.tlsUser(CLUSTER_NAME, kafkaUsername).done();

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
                .withTopicName(topicName)
                .withNamespaceName(NAMESPACE)
                .withClusterName(CLUSTER_NAME)
                .withMessageCount(MESSAGE_COUNT)
                .withKafkaUsername(kafkaUsername)
                .withSecurityProtocol(SecurityProtocol.SSL)
                .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
                basicExternalKafkaClient.sendMessagesTls(),
                basicExternalKafkaClient.receiveMessagesTls()
        );
    }

    @BeforeAll
    void setup() throws Exception {
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE);
    }

    @Override
    protected void tearDownEnvironmentAfterEach() throws Exception {
        super.tearDownEnvironmentAfterEach();
        kubeClient().getClient().persistentVolumeClaims().inNamespace(NAMESPACE).delete();
    }
}
