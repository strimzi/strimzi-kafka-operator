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
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.kafkaclients.externalClients.BasicExternalKafkaClient;
import io.strimzi.systemtest.resources.kubernetes.ServiceResource;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaBridgeUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
@Tag(BRIDGE)
@Tag(NODEPORT_SUPPORTED)
@Tag(EXTERNAL_CLIENTS_USED)
class HttpBridgeKafkaExternalListenersST extends HttpBridgeAbstractST {

    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeKafkaExternalListenersST.class);
    private static final String BRIDGE_EXTERNAL_SERVICE =  "shared-http-bridge-external-service";

    @IsolatedTest
    void testScramShaAuthWithWeirdUsername(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        // Create weird named user with . and more than 64 chars -> SCRAM-SHA
        String weirdUserName = "jjglmahyijoambryleyxjjglmahy.ijoambryleyxjjglmahyijoambryleyxasd.asdasidioiqweioqiweooioqieioqieoqieooi";

        // Initialize PasswordSecret to set this as PasswordSecret in Mirror Maker spec
        PasswordSecretSource passwordSecret = new PasswordSecretSource();
        passwordSecret.setSecretName(weirdUserName);
        passwordSecret.setPassword("password");

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecret = new CertSecretSource();
        certSecret.setCertificate("ca.crt");
        certSecret.setSecretName(KafkaResources.clusterCaCertificateSecretName(clusterName));

        KafkaBridgeSpec bridgeSpec = new KafkaBridgeSpecBuilder()
            .withNewKafkaClientAuthenticationScramSha512()
                .withNewUsername(weirdUserName)
                .withPasswordSecret(passwordSecret)
            .endKafkaClientAuthenticationScramSha512()
            .withNewTls()
                .withTrustedCertificates(certSecret)
            .endTls()
            .build();

        testWeirdUsername(extensionContext, weirdUserName, new KafkaListenerAuthenticationScramSha512(), bridgeSpec, SecurityProtocol.SASL_SSL);
    }

    @IsolatedTest
    void testTlsAuthWithWeirdUsername(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        // Create weird named user with . and maximum of 64 chars -> TLS
        String weirdUserName = "jjglmahyijoambryleyxjjglmahy.ijoambryleyxjjglmahyijoambryleyxasd";

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

    private void testWeirdUsername(ExtensionContext extensionContext, String weirdUserName, KafkaListenerAuthentication auth, KafkaBridgeSpec spec, SecurityProtocol securityProtocol) {
        String aliceUser = mapWithClusterNames.get(extensionContext.getDisplayName());
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withAuth(auth)
                        .endGenericKafkaListener()
                        .addNewGenericKafkaListener()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9094)
                            .withType(KafkaListenerType.NODEPORT)
                            .withTls(true)
                            .withAuth(auth)
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec().build());

        kafkaBridgeClientJob = kafkaBridgeClientJob.toBuilder()
            .withBootstrapAddress(KafkaBridgeResources.serviceName(clusterName))
            .withProducerName(clusterName + "-" + producerName)
            .withConsumerName(clusterName + "-" + consumerName)
            .withTopicName(topicName)
            .build();

        // Create topic
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());

        // Create user
        if (auth.getType().equals(Constants.TLS_LISTENER_DEFAULT_NAME)) {
            resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(clusterName, weirdUserName).build());
            resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(clusterName, aliceUser).build());
        } else {
            resourceManager.createResource(extensionContext, KafkaUserTemplates.scramShaUser(clusterName, weirdUserName).build());
            resourceManager.createResource(extensionContext, KafkaUserTemplates.scramShaUser(clusterName, aliceUser).build());
        }

        String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, kafkaClientsName).build());

        // Deploy http bridge
        resourceManager.createResource(extensionContext, KafkaBridgeTemplates.kafkaBridge(clusterName, KafkaResources.tlsBootstrapAddress(clusterName), 1)
                .withNewSpecLike(spec)
                    .withBootstrapServers(KafkaResources.tlsBootstrapAddress(clusterName))
                    .withNewHttp(Constants.HTTP_BRIDGE_DEFAULT_PORT)
                .withNewConsumer()
                    .addToConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .endConsumer()
            .endSpec()
            .build());

        Service service = KafkaBridgeUtils.createBridgeNodePortService(clusterName, NAMESPACE, BRIDGE_EXTERNAL_SERVICE);
        ServiceResource.createServiceResource(extensionContext, service, NAMESPACE);

        resourceManager.createResource(extensionContext, kafkaBridgeClientJob.consumerStrimziBridge().build());

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withClusterName(clusterName)
            .withNamespaceName(NAMESPACE)
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(weirdUserName)
            .withSecurityProtocol(securityProtocol)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        assertThat(basicExternalKafkaClient.sendMessagesTls(), is(MESSAGE_COUNT));
        ClientUtils.waitForClientSuccess(clusterName + "-" + consumerName, NAMESPACE, MESSAGE_COUNT);
    }

    @BeforeAll
    void createClassResources(ExtensionContext extensionContext) {
        LOGGER.debug("===============================================================");
        LOGGER.debug("{} - [BEFORE ALL] has been called", this.getClass().getName());
        installClusterOperator(extensionContext, NAMESPACE);
    }
}
