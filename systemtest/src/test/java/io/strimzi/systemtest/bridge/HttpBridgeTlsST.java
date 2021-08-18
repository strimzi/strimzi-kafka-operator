/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.ParallelSuite;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.parallel.ParallelSuiteController;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBridgeExampleClients;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.Random;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
@Tag(BRIDGE)
@Tag(ACCEPTANCE)
@Tag(INTERNAL_CLIENTS_USED)
@ParallelSuite
class HttpBridgeTlsST extends HttpBridgeAbstractST {
    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeTlsST.class);
    private static final String NAMESPACE = "http-bridge-tls-namespace";
    private final String httpBridgeTlsClusterName = "http-bridge-tls-cluster-name";
    private KafkaBridgeExampleClients kafkaBridgeClientJob;
    private String kafkaClientsPodName;

    private final String producerName = "producer-" + new Random().nextInt(Integer.MAX_VALUE);
    private final String consumerName = "consumer-" + new Random().nextInt(Integer.MAX_VALUE);

    @ParallelTest
    void testSendSimpleMessageTls(ExtensionContext extensionContext) {
        // Create topic
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
        KafkaBridgeExampleClients kafkaBridgeClientJobProduce = kafkaBridgeClientJob.toBuilder().withTopicName(topicName).build();

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(httpBridgeTlsClusterName, topicName)
            .editMetadata()
                .withNamespace(NAMESPACE)
            .endMetadata()
            .build());
        resourceManager.createResource(extensionContext, kafkaBridgeClientJobProduce.producerStrimziBridge()
            .editMetadata()
                .withNamespace(NAMESPACE)
            .endMetadata()
            .build());

        ClientUtils.waitForClientSuccess(producerName, NAMESPACE, MESSAGE_COUNT);

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(httpBridgeTlsClusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withKafkaUsername(USER_NAME)
            .withUsingPodName(kafkaClientsPodName)
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        assertThat(internalKafkaClient.receiveMessagesTls(), is(MESSAGE_COUNT));
    }

    @ParallelTest
    void testReceiveSimpleMessageTls(ExtensionContext extensionContext) {
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
        KafkaBridgeExampleClients kafkaBridgeClientJobConsume = kafkaBridgeClientJob.toBuilder().withTopicName(topicName).build();

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(httpBridgeTlsClusterName, topicName)
            .editMetadata()
                .withNamespace(NAMESPACE)
            .endMetadata()
            .build());
        resourceManager.createResource(extensionContext, kafkaBridgeClientJobConsume.consumerStrimziBridge()
            .editMetadata()
                .withNamespace(NAMESPACE)
            .endMetadata()
            .build());

        // Send messages to Kafka
        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(httpBridgeTlsClusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withKafkaUsername(USER_NAME)
            .withUsingPodName(kafkaClientsPodName)
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        assertThat(internalKafkaClient.sendMessagesTls(), is(MESSAGE_COUNT));

        ClientUtils.waitForClientSuccess(consumerName, NAMESPACE, MESSAGE_COUNT);
    }

    @BeforeAll
    void createClassResources(ExtensionContext extensionContext) {
        ParallelSuiteController.addParallelSuite(extensionContext);

        cluster.createNamespace(extensionContext, NAMESPACE);

        LOGGER.info("Deploy Kafka and KafkaBridge before tests");

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(httpBridgeTlsClusterName, 1, 1)
            .editMetadata()
                .withNamespace(NAMESPACE)
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewKafkaListenerAuthenticationTlsAuth()
                            .endKafkaListenerAuthenticationTlsAuth()
                            .build())
                .endKafka()
            .endSpec()
            .build());

        // Create Kafka user
        KafkaUser tlsUser = KafkaUserTemplates.tlsUser(httpBridgeTlsClusterName, USER_NAME)
            .editMetadata()
                .withNamespace(NAMESPACE)
            .endMetadata().build();
        resourceManager.createResource(extensionContext, tlsUser);

        String kafkaClientsName = NAMESPACE + "-" + Constants.KAFKA_CLIENTS;

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, kafkaClientsName, tlsUser)
            .editMetadata()
                .withNamespace(NAMESPACE)
            .endMetadata().build());

        kafkaClientsPodName = kubeClient(NAMESPACE).listPodsByPrefixInName(NAMESPACE, kafkaClientsName).get(0).getMetadata().getName();

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecret = new CertSecretSource();
        certSecret.setCertificate("ca.crt");
        certSecret.setSecretName(KafkaResources.clusterCaCertificateSecretName(httpBridgeTlsClusterName));

        // Deploy http bridge
        resourceManager.createResource(extensionContext, KafkaBridgeTemplates.kafkaBridge(httpBridgeTlsClusterName, KafkaResources.tlsBootstrapAddress(httpBridgeTlsClusterName), 1)
            .editMetadata()
                .withNamespace(NAMESPACE)
            .endMetadata()
            .editSpec()
                .withNewConsumer()
                    .addToConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .endConsumer()
                .withNewKafkaClientAuthenticationTls()
                    .withNewCertificateAndKey()
                        .withSecretName(USER_NAME)
                        .withCertificate("user.crt")
                        .withKey("user.key")
                    .endCertificateAndKey()
                .endKafkaClientAuthenticationTls()
                .withNewTls()
                    .withTrustedCertificates(certSecret)
                .endTls()
            .endSpec()
            .build());

        kafkaBridgeClientJob = (KafkaBridgeExampleClients) new KafkaBridgeExampleClients.Builder()
            .withBootstrapAddress(KafkaBridgeResources.serviceName(httpBridgeTlsClusterName))
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withTopicName(TOPIC_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withPort(bridgePort)
            .withDelayMs(1000)
            .withPollInterval(1000)
            .withNamespaceName(NAMESPACE)
            .build();
    }
}
