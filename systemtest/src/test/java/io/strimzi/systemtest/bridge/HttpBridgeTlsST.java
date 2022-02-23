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
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.ParallelSuite;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClients;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClientsBuilder;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Random;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

@Tag(REGRESSION)
@Tag(BRIDGE)
@Tag(ACCEPTANCE)
@Tag(INTERNAL_CLIENTS_USED)
@ParallelSuite
class HttpBridgeTlsST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeTlsST.class);
    private BridgeClients kafkaBridgeClientJob;
    private String kafkaClientsPodName;
    private String sharedKafkaUserName;

    private final String httpBridgeTlsClusterName = "http-bridge-tls-cluster-name";
    private final String namespace = testSuiteNamespaceManager.getMapOfAdditionalNamespaces().get(HttpBridgeTlsST.class.getSimpleName()).stream().findFirst().get();

    @ParallelTest
    void testSendSimpleMessageTls(ExtensionContext extensionContext) {

        final String producerName = "producer-" + new Random().nextInt(Integer.MAX_VALUE);
        final String consumerName = "consumer-" + new Random().nextInt(Integer.MAX_VALUE);

        // Create topic
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
        BridgeClients kafkaBridgeClientJobProduce = new BridgeClientsBuilder(kafkaBridgeClientJob)
            .withTopicName(topicName)
            .withProducerName(producerName)
            .build();

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(httpBridgeTlsClusterName, topicName)
            .editMetadata()
                .withNamespace(namespace)
            .endMetadata()
            .build());

        resourceManager.createResource(extensionContext, kafkaBridgeClientJobProduce.producerStrimziBridge());
        ClientUtils.waitForClientSuccess(producerName, namespace, MESSAGE_COUNT);

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(httpBridgeTlsClusterName))
            .withConsumerName(consumerName)
            .withNamespaceName(namespace)
            .withUserName(sharedKafkaUserName)
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.consumerTlsStrimzi(httpBridgeTlsClusterName));
        ClientUtils.waitForClientSuccess(consumerName, namespace, MESSAGE_COUNT);
    }

    @ParallelTest
    void testReceiveSimpleMessageTls(ExtensionContext extensionContext) {

        final String producerName = "producer-" + new Random().nextInt(Integer.MAX_VALUE);
        final String consumerName = "consumer-" + new Random().nextInt(Integer.MAX_VALUE);

        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
        BridgeClients kafkaBridgeClientJobConsume = new BridgeClientsBuilder(kafkaBridgeClientJob)
            .withTopicName(topicName)
            .withConsumerName(consumerName)
            .build();

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(httpBridgeTlsClusterName, topicName)
            .editMetadata()
                .withNamespace(namespace)
            .endMetadata()
            .build());

        resourceManager.createResource(extensionContext, kafkaBridgeClientJobConsume.consumerStrimziBridge());

        // Send messages to Kafka
        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(httpBridgeTlsClusterName))
            .withProducerName(producerName)
            .withNamespaceName(namespace)
            .withUserName(sharedKafkaUserName)
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerTlsStrimzi(httpBridgeTlsClusterName));
        ClientUtils.waitForClientsSuccess(producerName, consumerName, namespace, MESSAGE_COUNT);
    }

    @BeforeAll
    void setUp(ExtensionContext extensionContext) {
        LOGGER.info("Deploy Kafka and KafkaBridge before tests");
        sharedKafkaUserName = KafkaUserUtils.generateRandomNameOfKafkaUser();

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(httpBridgeTlsClusterName, 1, 1)
            .editMetadata()
                .withNamespace(namespace)
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
        KafkaUser tlsUser = KafkaUserTemplates.tlsUser(namespace, httpBridgeTlsClusterName, sharedKafkaUserName).build();
        resourceManager.createResource(extensionContext, tlsUser);

        String kafkaClientsName = namespace + "-" + Constants.KAFKA_CLIENTS;

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, kafkaClientsName, tlsUser)
            .editMetadata()
                .withNamespace(namespace)
            .endMetadata().build());

        kafkaClientsPodName = kubeClient(namespace).listPodsByPrefixInName(namespace, kafkaClientsName).get(0).getMetadata().getName();

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecret = new CertSecretSource();
        certSecret.setCertificate("ca.crt");
        certSecret.setSecretName(KafkaResources.clusterCaCertificateSecretName(httpBridgeTlsClusterName));

        // Deploy http bridge
        resourceManager.createResource(extensionContext, KafkaBridgeTemplates.kafkaBridge(httpBridgeTlsClusterName, KafkaResources.tlsBootstrapAddress(httpBridgeTlsClusterName), 1)
            .editMetadata()
                .withNamespace(namespace)
            .endMetadata()
            .editSpec()
                .withNewConsumer()
                    .addToConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .endConsumer()
                .withNewKafkaClientAuthenticationTls()
                    .withNewCertificateAndKey()
                        .withSecretName(sharedKafkaUserName)
                        .withCertificate("user.crt")
                        .withKey("user.key")
                    .endCertificateAndKey()
                .endKafkaClientAuthenticationTls()
                .withNewTls()
                    .withTrustedCertificates(certSecret)
                .endTls()
            .endSpec()
            .build());

        kafkaBridgeClientJob = new BridgeClientsBuilder()
            .withBootstrapAddress(KafkaBridgeResources.serviceName(httpBridgeTlsClusterName))
            .withTopicName(TOPIC_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withPort(Constants.HTTP_BRIDGE_DEFAULT_PORT)
            .withNamespaceName(namespace)
            .build();
    }
}
