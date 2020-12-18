/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.KafkaBridgeSpec;
import io.strimzi.api.kafka.model.KafkaBridgeSpecBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.PasswordSecretSource;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthentication;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.externalClients.BasicExternalKafkaClient;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaBridgeUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

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
    private static final String BRIDGE_EXTERNAL_SERVICE = CLUSTER_NAME + "-bridge-external-service";

    @Test
    void testScramShaAuthWithWeirdUsername() {
        // Create weird named user with . and more than 64 chars -> SCRAM-SHA
        String weirdUserName = "jjglmahyijoambryleyxjjglmahy.ijoambryleyxjjglmahyijoambryleyxasd.asdasidioiqweioqiweooioqieioqieoqieooi";

        // Initialize PasswordSecret to set this as PasswordSecret in Mirror Maker spec
        PasswordSecretSource passwordSecret = new PasswordSecretSource();
        passwordSecret.setSecretName(weirdUserName);
        passwordSecret.setPassword("password");

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecret = new CertSecretSource();
        certSecret.setCertificate("ca.crt");
        certSecret.setSecretName(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME));

        KafkaBridgeSpec bridgeSpec = new KafkaBridgeSpecBuilder()
            .withNewKafkaClientAuthenticationScramSha512()
                .withNewUsername(weirdUserName)
                .withPasswordSecret(passwordSecret)
            .endKafkaClientAuthenticationScramSha512()
            .withNewTls()
                .withTrustedCertificates(certSecret)
            .endTls()
            .build();

        testWeirdUsername(weirdUserName, new KafkaListenerAuthenticationScramSha512(), bridgeSpec, SecurityProtocol.SASL_SSL);
    }

    @Test
    void testTlsAuthWithWeirdUsername() {
        // Create weird named user with . and maximum of 64 chars -> TLS
        String weirdUserName = "jjglmahyijoambryleyxjjglmahy.ijoambryleyxjjglmahyijoambryleyxasd";

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecret = new CertSecretSource();
        certSecret.setCertificate("ca.crt");
        certSecret.setSecretName(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME));

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

        testWeirdUsername(weirdUserName, new KafkaListenerAuthenticationTls(), bridgeSpec, SecurityProtocol.SSL);
    }

    private void testWeirdUsername(String weirdUserName, KafkaListenerAuthentication auth, KafkaBridgeSpec spec, SecurityProtocol securityProtocol) {
        String aliceUser = "alice";

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3)
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
                .endSpec()
            .done();

        // Create topic
        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME).done();

        // Create user
        if (auth.getType().equals(Constants.TLS_LISTENER_DEFAULT_NAME)) {
            KafkaUserResource.tlsUser(CLUSTER_NAME, weirdUserName).done();
            KafkaUserResource.tlsUser(CLUSTER_NAME, aliceUser).done();
        } else {
            KafkaUserResource.scramShaUser(CLUSTER_NAME, weirdUserName).done();
            KafkaUserResource.scramShaUser(CLUSTER_NAME, aliceUser).done();
        }

        KafkaClientsResource.deployKafkaClients(true, KAFKA_CLIENTS_NAME).done();

        // Deploy http bridge
        KafkaBridgeResource.kafkaBridge(CLUSTER_NAME, KafkaResources.tlsBootstrapAddress(CLUSTER_NAME), 1)
            .withNewSpecLike(spec)
                .withBootstrapServers(KafkaResources.tlsBootstrapAddress(CLUSTER_NAME))
                .withNewHttp(Constants.HTTP_BRIDGE_DEFAULT_PORT)
                .withNewConsumer()
                    .addToConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .endConsumer()
            .endSpec()
            .done();

        Service service = KafkaBridgeUtils.createBridgeNodePortService(CLUSTER_NAME, NAMESPACE, BRIDGE_EXTERNAL_SERVICE);
        KubernetesResource.createServiceResource(service, NAMESPACE).done();

        kafkaBridgeClientJob.consumerStrimziBridge().done();

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withClusterName(CLUSTER_NAME)
            .withNamespaceName(NAMESPACE)
            .withTopicName(TOPIC_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(weirdUserName)
            .withSecurityProtocol(securityProtocol)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        assertThat(basicExternalKafkaClient.sendMessagesTls(), is(MESSAGE_COUNT));
        ClientUtils.waitForClientSuccess(consumerName, NAMESPACE, MESSAGE_COUNT);
    }

    @BeforeAll
    void createClassResources() throws Exception {
        deployClusterOperator(NAMESPACE);
    }
}
