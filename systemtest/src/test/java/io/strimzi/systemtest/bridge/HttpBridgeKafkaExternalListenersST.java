/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeResources;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeSpec;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeSpecBuilder;
import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.api.kafka.model.common.PasswordSecretSource;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthentication;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.kafka.listener.ListenerStatus;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClients;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClientsBuilder;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.kubernetes.ServiceResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaBridgeUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.List;
import java.util.Random;

import static io.strimzi.systemtest.TestConstants.BRIDGE;
import static io.strimzi.systemtest.TestConstants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.TestConstants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.TestConstants.REGRESSION;

@Tag(REGRESSION)
@Tag(BRIDGE)
@Tag(NODEPORT_SUPPORTED)
@Tag(EXTERNAL_CLIENTS_USED)
public class HttpBridgeKafkaExternalListenersST extends AbstractST {
    private static final String BRIDGE_EXTERNAL_SERVICE =  "shared-http-bridge-external-service";
    private final String producerName = "producer-" + new Random().nextInt(Integer.MAX_VALUE);
    private final String consumerName = "consumer-" + new Random().nextInt(Integer.MAX_VALUE);

    @ParallelNamespaceTest("Creating a node port service and thus avoiding 409 error (service already exists)")
    void testScramShaAuthWithWeirdUsername() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        // Create weird named user with . and more than 64 chars -> SCRAM-SHA
        final String weirdUserName = "jjglmahyijoambryleyxjjglmahy.ijoambryleyxjjglmahyijoambryleyxasd.asdasidioiqweioqiweooioqieioqieoqieooi";

        // Initialize PasswordSecret to set this as PasswordSecret in MirrorMaker spec
        final PasswordSecretSource passwordSecret = new PasswordSecretSource();
        passwordSecret.setSecretName(weirdUserName);
        passwordSecret.setPassword("password");

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecret = new CertSecretSource();
        certSecret.setCertificate("ca.crt");
        certSecret.setSecretName(KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()));

        KafkaBridgeSpec bridgeSpec = new KafkaBridgeSpecBuilder()
            .withNewKafkaClientAuthenticationScramSha512()
                .withUsername(weirdUserName)
                .withPasswordSecret(passwordSecret)
            .endKafkaClientAuthenticationScramSha512()
            .withNewTls()
                .withTrustedCertificates(certSecret)
            .endTls()
            .build();

        testWeirdUsername(weirdUserName, new KafkaListenerAuthenticationScramSha512(), bridgeSpec, testStorage);
    }

    @ParallelNamespaceTest("Creating a node port service and thus avoiding 409 error")
    void testTlsAuthWithWeirdUsername() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        // Create weird named user with . and maximum of 64 chars -> TLS
        final String weirdUserName = "jjglmahyijoambryleyxjjglmahy.ijoambryleyxjjglmahyijoambryleyxasd";

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecret = new CertSecretSource();
        certSecret.setCertificate("ca.crt");
        certSecret.setSecretName(KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()));

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

        testWeirdUsername(weirdUserName, new KafkaListenerAuthenticationTls(), bridgeSpec, testStorage);
    }

    @SuppressWarnings({"checkstyle:MethodLength"})
    private void testWeirdUsername(String weirdUserName, KafkaListenerAuthentication auth,
                                   KafkaBridgeSpec spec, TestStorage testStorage) {

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3, 1)
            .editMetadata()
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .editSpec()
                .editKafka()
                .withListeners(new GenericKafkaListenerBuilder()
                        .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                        .withPort(9093)
                        .withType(KafkaListenerType.INTERNAL)
                        .withTls(true)
                        .withAuth(auth)
                        .build(),
                    new GenericKafkaListenerBuilder()
                        .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
                        .withPort(9094)
                        .withType(KafkaListenerType.NODEPORT)
                        .withTls(true)
                        .withAuth(auth)
                        .build())
                .endKafka()
            .endSpec()
            .build());

        BridgeClients kafkaBridgeClientJob = new BridgeClientsBuilder()
            .withProducerName(testStorage.getClusterName() + "-" + producerName)
            .withConsumerName(testStorage.getClusterName() + "-" + consumerName)
            .withBootstrapAddress(KafkaBridgeResources.serviceName(testStorage.getClusterName()))
            .withComponentName(KafkaBridgeResources.componentName(testStorage.getClusterName()))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withPort(TestConstants.HTTP_BRIDGE_DEFAULT_PORT)
            .withNamespaceName(testStorage.getNamespaceName())
            .build();

        // Create topic
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());

        // Create user
        if (auth.getType().equals(TestConstants.TLS_LISTENER_DEFAULT_NAME)) {
            resourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), testStorage.getClusterName(), weirdUserName)
                .editMetadata()
                    .withNamespace(testStorage.getNamespaceName())
                .endMetadata()
                .build());
        } else {
            resourceManager.createResourceWithWait(KafkaUserTemplates.scramShaUser(testStorage.getNamespaceName(), testStorage.getClusterName(), weirdUserName)
                .editMetadata()
                    .withNamespace(testStorage.getNamespaceName())
                .endMetadata()
                .build());
        }

        // Deploy http bridge
        resourceManager.createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(testStorage.getClusterName(), KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()), 1)
                .editMetadata()
                    .withNamespace(testStorage.getNamespaceName())
                .endMetadata()
                .withNewSpecLike(spec)
                    .withBootstrapServers(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
                    .withNewHttp(TestConstants.HTTP_BRIDGE_DEFAULT_PORT)
                .withNewConsumer()
                    .addToConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .endConsumer()
            .endSpec()
            .build());

        final Service service = KafkaBridgeUtils.createBridgeNodePortService(testStorage.getClusterName(), testStorage.getNamespaceName(), BRIDGE_EXTERNAL_SERVICE);
        ServiceResource.createServiceResource(service, testStorage.getNamespaceName());

        resourceManager.createResourceWithWait(kafkaBridgeClientJob.consumerStrimziBridge());

        final String kafkaProducerExternalName = "kafka-producer-external" + new Random().nextInt(Integer.MAX_VALUE);

        final List<ListenerStatus> listenerStatusList = KafkaResource.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus().getListeners();
        final String externalBootstrapServers = listenerStatusList.stream().filter(listener -> listener.getName().equals(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME))
            .findFirst()
            .orElseThrow(RuntimeException::new)
            .getBootstrapServers();

        final KafkaClients externalKafkaProducer = new KafkaClientsBuilder()
            .withProducerName(kafkaProducerExternalName)
            .withBootstrapAddress(externalBootstrapServers)
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withUsername(weirdUserName)
            // we disable ssl.endpoint.identification.algorithm for external listener (i.e., NodePort),
            // because TLS hostname verification is not supported on such listener type.
            .withAdditionalConfig("ssl.endpoint.identification.algorithm=\n")
            .build();

        if (auth.getType().equals(TestConstants.TLS_LISTENER_DEFAULT_NAME)) {
            // tls producer
            resourceManager.createResourceWithWait(externalKafkaProducer.producerTlsStrimzi(testStorage.getClusterName()));
        } else {
            // scram-sha producer
            resourceManager.createResourceWithWait(externalKafkaProducer.producerScramShaTlsStrimzi(testStorage.getClusterName()));
        }

        ClientUtils.waitForClientSuccess(kafkaProducerExternalName, testStorage.getNamespaceName(), testStorage.getMessageCount());

        ClientUtils.waitForClientSuccess(testStorage.getClusterName() + "-" + consumerName, testStorage.getNamespaceName(), testStorage.getMessageCount());
    }

    @BeforeAll
    void setUp() {
        clusterOperator = clusterOperator.defaultInstallation()
            .createInstallation()
            .runInstallation();
    }

}
