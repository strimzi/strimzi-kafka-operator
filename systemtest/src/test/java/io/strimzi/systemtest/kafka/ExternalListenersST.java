/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka;

import io.strimzi.systemtest.MessagingBaseST;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class ExternalListenersST extends MessagingBaseST {
    private static final Logger LOGGER = LogManager.getLogger(ExternalListenersST.class);

    public static final String NAMESPACE = "certs-cluster-test";

    @Test
    void testCustomSoloCertificatesForNodePort() throws Exception {
        String topicName = "test-topic-" + rng.nextInt(Integer.MAX_VALUE);
        createCustomSoloSecrets();

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 3)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewKafkaListenerExternalNodePort()
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName("custom-certificate")
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .endKafkaListenerExternalNodePort()
                    .endListeners()
                .endKafka()
            .endSpec().done();


        sendMessagesExternalTls(NAMESPACE, topicName, 10, "", "custom-certificate");
        receiveMessagesExternalTls(NAMESPACE, topicName, 10, "", "consumer-group-certs-1", "custom-certificate");
    }

    @Test
    void testCustomChainCertificatesForNodePort() throws Exception {
        String topicName = "test-topic-" + rng.nextInt(Integer.MAX_VALUE);
        createCustomSecretsFromChain();

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 3)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewKafkaListenerExternalNodePort()
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName("custom-certificate")
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .endKafkaListenerExternalNodePort()
                    .endListeners()
                .endKafka()
            .endSpec().done();


        sendMessagesExternalTls(NAMESPACE, topicName, 10, "", "custom-certificate");
        receiveMessagesExternalTls(NAMESPACE, topicName, 10, "", "consumer-group-certs-2", "custom-certificate");
    }

    @Disabled
    @Test
    void testCustomSoloCertificatesForLoadBalancer() throws Exception {
        String topicName = "test-topic-" + rng.nextInt(Integer.MAX_VALUE);
        createCustomSoloSecrets();

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 3)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewKafkaListenerExternalLoadBalancer()
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName("custom-certificate")
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .endKafkaListenerExternalLoadBalancer()
                    .endListeners()
                .endKafka()
            .endSpec().done();


        sendMessagesExternalTls(NAMESPACE, topicName, 10, "", "custom-certificate");
        receiveMessagesExternalTls(NAMESPACE, topicName, 10, "", "consumer-group-certs-3", "custom-certificate");
    }

    @Disabled
    @Test
    void testCustomChainCertificatesForLoadBalancer() throws Exception {
        String topicName = "test-topic-" + rng.nextInt(Integer.MAX_VALUE);
        createCustomSecretsFromChain();

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 3)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewKafkaListenerExternalLoadBalancer()
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName("custom-certificate")
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .endKafkaListenerExternalLoadBalancer()
                    .endListeners()
                .endKafka()
            .endSpec().done();


        sendMessagesExternalTls(NAMESPACE, topicName, 10, "", "custom-certificate");
        receiveMessagesExternalTls(NAMESPACE, topicName, 10, "", "consumer-group-certs-4", "custom-certificate");
    }

    @Test
    void testCustomSoloCertificatesForRoute() throws Exception {
        String topicName = "test-topic-" + rng.nextInt(Integer.MAX_VALUE);
        createCustomSoloSecrets();

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 3)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewKafkaListenerExternalRoute()
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName("custom-certificate")
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .endKafkaListenerExternalRoute()
                    .endListeners()
                .endKafka()
            .endSpec().done();


        sendMessagesExternalTls(NAMESPACE, topicName, 10, "", "custom-certificate");
        receiveMessagesExternalTls(NAMESPACE, topicName, 10, "", "consumer-group-certs-5", "custom-certificate");
    }

    @Test
    void testCustomChainCertificatesForRoute() throws Exception {
        String topicName = "test-topic-" + rng.nextInt(Integer.MAX_VALUE);
        createCustomSecretsFromChain();

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 3)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewKafkaListenerExternalRoute()
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName("custom-certificate")
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .endKafkaListenerExternalRoute()
                    .endListeners()
                .endKafka()
            .endSpec().done();


        sendMessagesExternalTls(NAMESPACE, topicName, 10, "", "custom-certificate");
        receiveMessagesExternalTls(NAMESPACE, topicName, 10, "", "consumer-group-certs-6", "custom-certificate");
    }



    private void createCustomSoloSecrets() {
        Map<String, String> secretLabels = new HashMap<>();
        secretLabels.put("strimzi.io/cluster", CLUSTER_NAME);
        secretLabels.put("strimzi.io/kind", "Kafka");

        Map<String, String> certsPaths = new HashMap<>();
        certsPaths.put("ca.crt", getClass().getClassLoader().getResource("custom-certs/strimzi-bundle.crt").getFile());
        certsPaths.put("ca.key", getClass().getClassLoader().getResource("custom-certs/strimzi-key.pem").getFile());

        SecretUtils.createSecretFromFile(certsPaths, "custom-certificate", NAMESPACE, secretLabels);
    }

    private void createCustomSecretsFromChain() {
        Map<String, String> secretLabels = new HashMap<>();
        secretLabels.put("strimzi.io/cluster", CLUSTER_NAME);
        secretLabels.put("strimzi.io/kind", "Kafka");

        Map<String, String> certsPaths2 = new HashMap<>();
        certsPaths2.put("ca.crt", getClass().getClassLoader().getResource("custom-certs/strimzi.pem").getFile());
        certsPaths2.put("ca.key", getClass().getClassLoader().getResource("custom-certs/strimzi.key").getFile());

        SecretUtils.createSecretFromFile(certsPaths2, "custom-certificate", NAMESPACE, secretLabels);
    }

    @BeforeAll
    void setup() {
        ResourceManager.setClassResources();

        prepareEnvForOperator(NAMESPACE);

        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        KubernetesResource.clusterOperator(NAMESPACE).done();
    }

    @Override
    protected void tearDownEnvironmentAfterEach() throws Exception {
        super.tearDownEnvironmentAfterEach();
        kubeClient().getClient().persistentVolumeClaims().inNamespace(NAMESPACE).delete();
    }
}
