/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaBridgeSpec;
import io.strimzi.api.kafka.model.KafkaBridgeSpecBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.PasswordSecretSource;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthentication;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.ListenerStatus;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.annotations.ParallelSuite;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClients;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClientsBuilder;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.kubernetes.ServiceResource;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaBridgeUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.List;
import java.util.Random;

import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;

@Tag(REGRESSION)
@Tag(BRIDGE)
@Tag(NODEPORT_SUPPORTED)
@Tag(EXTERNAL_CLIENTS_USED)
@KRaftNotSupported("Scram-sha is not supported by KRaft mode and is used in this test class")
@ParallelSuite
public class HttpBridgeKafkaExternalListenersST extends AbstractST {
    private static final String BRIDGE_EXTERNAL_SERVICE =  "shared-http-bridge-external-service";

    private final String namespace = testSuiteNamespaceManager.getMapOfAdditionalNamespaces().get(HttpBridgeKafkaExternalListenersST.class.getSimpleName()).stream().findFirst().get();
    private final String producerName = "producer-" + new Random().nextInt(Integer.MAX_VALUE);
    private final String consumerName = "consumer-" + new Random().nextInt(Integer.MAX_VALUE);

    @ParallelTest
    void testScramShaAuthWithWeirdUsername(ExtensionContext extensionContext) {
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        // Create weird named user with . and more than 64 chars -> SCRAM-SHA
        final String weirdUserName = "jjglmahyijoambryleyxjjglmahy.ijoambryleyxjjglmahyijoambryleyxasd.asdasidioiqweioqiweooioqieioqieoqieooi";

        // Initialize PasswordSecret to set this as PasswordSecret in Mirror Maker spec
        final PasswordSecretSource passwordSecret = new PasswordSecretSource();
        passwordSecret.setSecretName(weirdUserName);
        passwordSecret.setPassword("password");

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecret = new CertSecretSource();
        certSecret.setCertificate("ca.crt");
        certSecret.setSecretName(KafkaResources.clusterCaCertificateSecretName(clusterName));

        KafkaBridgeSpec bridgeSpec = new KafkaBridgeSpecBuilder()
            .withNewKafkaClientAuthenticationScramSha512()
                .withUsername(weirdUserName)
                .withPasswordSecret(passwordSecret)
            .endKafkaClientAuthenticationScramSha512()
            .withNewTls()
                .withTrustedCertificates(certSecret)
            .endTls()
            .build();

        testWeirdUsername(extensionContext, weirdUserName, new KafkaListenerAuthenticationScramSha512(), bridgeSpec, SecurityProtocol.SASL_SSL);
    }

    @ParallelTest
    void testTlsAuthWithWeirdUsername(ExtensionContext extensionContext) {
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        // Create weird named user with . and maximum of 64 chars -> TLS
        final String weirdUserName = "jjglmahyijoambryleyxjjglmahy.ijoambryleyxjjglmahyijoambryleyxasd";

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecret = new CertSecretSource();
        certSecret.setCertificate("ca.crt");
        certSecret.setSecretName(KafkaResources.clusterCaCertificateSecretName(clusterName));

        KafkaBridgeSpec bridgeSpec = new KafkaBridgeSpecBuilder()
            .withNewKafkaClientAuthenticationTls()
                .withNewCertificateAndKey()
                    .withSecretName(weirdUserName)
                    .withCertificate("user.crt")
                    .withKey("user.key")
                .endCertificateAndKey()
            .endKafkaClientAuthenticationTls()
            .withNewTls()
                .withTrustedCertificates(certSecret)
            .endTls()
            .build();

        testWeirdUsername(extensionContext, weirdUserName, new KafkaListenerAuthenticationTls(), bridgeSpec, SecurityProtocol.SSL);
    }

    @SuppressWarnings({"checkstyle:MethodLength"})
    private void testWeirdUsername(ExtensionContext extensionContext, String weirdUserName, KafkaListenerAuthentication auth, KafkaBridgeSpec spec, SecurityProtocol securityProtocol) {
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3, 1)
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
                        .withAuth(auth)
                        .build(),
                    new GenericKafkaListenerBuilder()
                        .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                        .withPort(9094)
                        .withType(KafkaListenerType.NODEPORT)
                        .withTls(true)
                        .withAuth(auth)
                        .build())
                .endKafka()
            .endSpec()
            .build());

        BridgeClients kafkaBridgeClientJob = new BridgeClientsBuilder()
            .withProducerName(clusterName + "-" + producerName)
            .withConsumerName(clusterName + "-" + consumerName)
            .withBootstrapAddress(KafkaBridgeResources.serviceName(clusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withPort(Constants.HTTP_BRIDGE_DEFAULT_PORT)
            .withNamespaceName(namespace)
            .build();

        // Create topic
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName)
            .editMetadata()
                .withNamespace(namespace)
            .endMetadata()
            .build());

        // Create user
        if (auth.getType().equals(Constants.TLS_LISTENER_DEFAULT_NAME)) {
            resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(namespace, clusterName, weirdUserName)
                .editMetadata()
                    .withNamespace(namespace)
                .endMetadata()
                .build());
        } else {
            resourceManager.createResource(extensionContext, KafkaUserTemplates.scramShaUser(namespace, clusterName, weirdUserName)
                .editMetadata()
                    .withNamespace(namespace)
                .endMetadata()
                .build());
        }

        // Deploy http bridge
        resourceManager.createResource(extensionContext, KafkaBridgeTemplates.kafkaBridge(clusterName, KafkaResources.tlsBootstrapAddress(clusterName), 1)
                .editMetadata()
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSpecLike(spec)
                    .withBootstrapServers(KafkaResources.tlsBootstrapAddress(clusterName))
                    .withNewHttp(Constants.HTTP_BRIDGE_DEFAULT_PORT)
                .withNewConsumer()
                    .addToConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .endConsumer()
            .endSpec()
            .build());

        final Service service = KafkaBridgeUtils.createBridgeNodePortService(clusterName, namespace, BRIDGE_EXTERNAL_SERVICE);
        ServiceResource.createServiceResource(extensionContext, service, namespace);

        resourceManager.createResource(extensionContext, kafkaBridgeClientJob.consumerStrimziBridge());

        final String kafkaProducerExternalName = "kafka-producer-external" + new Random().nextInt(Integer.MAX_VALUE);

        final List<ListenerStatus> listenerStatusList = KafkaResource.kafkaClient().inNamespace(namespace).withName(clusterName).get().getStatus().getListeners();
        final String externalBootstrapServers = listenerStatusList.stream().filter(listener -> listener.getType().equals(Constants.EXTERNAL_LISTENER_DEFAULT_NAME))
            .findFirst()
            .orElseThrow(RuntimeException::new)
            .getBootstrapServers();

        final KafkaClients externalKafkaProducer = new KafkaClientsBuilder()
            .withProducerName(kafkaProducerExternalName)
            .withBootstrapAddress(externalBootstrapServers)
            .withNamespaceName(namespace)
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withUserName(weirdUserName)
            // we disable ssl.endpoint.identification.algorithm for external listener (i.e., NodePort),
            // because TLS hostname verification is not supported on such listener type.
            .withAdditionalConfig("ssl.endpoint.identification.algorithm=\n")
            .build();

        if (auth.getType().equals(Constants.TLS_LISTENER_DEFAULT_NAME)) {
            // tls producer
            resourceManager.createResource(extensionContext, externalKafkaProducer.producerTlsStrimzi(clusterName));
        } else {
            // scram-sha producer
            resourceManager.createResource(extensionContext, externalKafkaProducer.producerScramShaTlsStrimzi(clusterName));
        }

        ClientUtils.waitForClientSuccess(kafkaProducerExternalName, namespace, MESSAGE_COUNT);

        ClientUtils.waitForClientSuccess(clusterName + "-" + consumerName, namespace, MESSAGE_COUNT);
    }
}
