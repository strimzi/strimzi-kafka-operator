/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.KafkaListenerExternalNodePortBuilder;
import io.strimzi.systemtest.MessagingBaseST;
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.strimzi.systemtest.Constants.LOADBALANCER_SUPPORTED;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static java.util.Collections.singletonMap;

public class ExternalListenersST extends MessagingBaseST {
    private static final Logger LOGGER = LogManager.getLogger(ExternalListenersST.class);

    public static final String NAMESPACE = "custom-certs-cluster-test";

    @Test
    @Tag(NODEPORT_SUPPORTED)
    void testCustomSoloCertificatesForNodePort() throws Exception {
        String topicName = "test-topic-" + rng.nextInt(Integer.MAX_VALUE);

        SecretUtils.createCustomSecret(CLUSTER_NAME, NAMESPACE,
                getClass().getClassLoader().getResource("custom-certs/strimzi.pem").getFile(),
                getClass().getClassLoader().getResource("custom-certs/strimzi.key").getFile());

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
    @Tag(NODEPORT_SUPPORTED)
    void testCustomChainCertificatesForNodePort() throws Exception {
        String topicName = "test-topic-" + rng.nextInt(Integer.MAX_VALUE);

        SecretUtils.createCustomSecret(CLUSTER_NAME, NAMESPACE,
                getClass().getClassLoader().getResource("custom-certs/strimzi-bundle.crt").getFile(),
                getClass().getClassLoader().getResource("custom-certs/strimzi-key.pem").getFile());

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

    @Test
    @Tag(LOADBALANCER_SUPPORTED)
    void testCustomSoloCertificatesForLoadBalancer() throws Exception {
        String topicName = "test-topic-" + rng.nextInt(Integer.MAX_VALUE);

        SecretUtils.createCustomSecret(CLUSTER_NAME, NAMESPACE,
                getClass().getClassLoader().getResource("custom-certs/strimzi.pem").getFile(),
                getClass().getClassLoader().getResource("custom-certs/strimzi.key").getFile());

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

    @Test
    @Tag(LOADBALANCER_SUPPORTED)
    void testCustomChainCertificatesForLoadBalancer() throws Exception {
        String topicName = "test-topic-" + rng.nextInt(Integer.MAX_VALUE);

        SecretUtils.createCustomSecret(CLUSTER_NAME, NAMESPACE,
                getClass().getClassLoader().getResource("custom-certs/strimzi-bundle.crt").getFile(),
                getClass().getClassLoader().getResource("custom-certs/strimzi-key.pem").getFile());

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
    @OpenShiftOnly
    void testCustomSoloCertificatesForRoute() throws Exception {
        String topicName = "test-topic-" + rng.nextInt(Integer.MAX_VALUE);

        SecretUtils.createCustomSecret(CLUSTER_NAME, NAMESPACE,
                getClass().getClassLoader().getResource("custom-certs/strimzi.pem").getFile(),
                getClass().getClassLoader().getResource("custom-certs/strimzi.key").getFile());

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
    @OpenShiftOnly
    void testCustomChainCertificatesForRoute() throws Exception {
        String topicName = "test-topic-" + rng.nextInt(Integer.MAX_VALUE);
        LOGGER.info(kubeClient().getClient().getConfiguration());

        SecretUtils.createCustomSecret(CLUSTER_NAME, NAMESPACE,
                getClass().getClassLoader().getResource("custom-certs/strimzi-bundle.crt").getFile(),
                getClass().getClassLoader().getResource("custom-certs/strimzi-key.pem").getFile());

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


    @Test
    @Tag(NODEPORT_SUPPORTED)
    void testCustomCertNodePortRollingUpdate() throws Exception {
        String topicName = "test-topic-" + rng.nextInt(Integer.MAX_VALUE);

        SecretUtils.createCustomSecret(CLUSTER_NAME, NAMESPACE,
                getClass().getClassLoader().getResource("custom-certs/strimzi-bundle.crt").getFile(),
                getClass().getClassLoader().getResource("custom-certs/strimzi-key.pem").getFile());

        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3, 3)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewKafkaListenerExternalNodePort()
                        .endKafkaListenerExternalNodePort()
                    .endListeners()
                .withConfig(singletonMap("default.replication.factor", 3))
                .endKafka()
            .endSpec().done();

        String userName = "alice";
        KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();

        sendMessagesExternalTls(NAMESPACE, topicName, 10, userName);
        receiveMessagesExternalTls(NAMESPACE, topicName, 10, userName, "consumer-group-certs-2");

        Map<String, String> kafkaSnapshot = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            kafka.getSpec().getKafka().getListeners().setExternal(new KafkaListenerExternalNodePortBuilder()
            .withNewConfiguration()
                    .withNewBrokerCertChainAndKey()
                        .withSecretName("custom-certificate")
                        .withKey("ca.key")
                        .withCertificate("ca.crt")
                    .endBrokerCertChainAndKey()
            .endConfiguration()
            .build());
        });

        kafkaSnapshot = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3);

        sendMessagesExternalTls(NAMESPACE, topicName, 10, "", "custom-certificate");
        receiveMessagesExternalTls(NAMESPACE, topicName, 20, "", "consumer-group-certs-66", "custom-certificate");

        SecretUtils.createCustomSecret(CLUSTER_NAME, NAMESPACE,
                getClass().getClassLoader().getResource("custom-certs/strimzi-bundle-2.crt").getFile(),
                getClass().getClassLoader().getResource("custom-certs/strimzi-key-2.pem").getFile());

        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3);

        sendMessagesExternalTls(NAMESPACE, topicName, 10, "", "custom-certificate");
        receiveMessagesExternalTls(NAMESPACE, topicName, 30, "", "consumer-group-certs-68", "custom-certificate");

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            kafka.getSpec().getKafka().getListeners().setExternal(new KafkaListenerExternalNodePortBuilder()
                .withTls(true)
                .build());
        });

        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3);

        sendMessagesExternalTls(NAMESPACE, topicName, 10, userName);
        receiveMessagesExternalTls(NAMESPACE, topicName, 40, userName, "consumer-group-certs-92");
    }

    @Test
    @Tag(LOADBALANCER_SUPPORTED)
    void testCustomCertLoadBalancerRollingUpdate() throws Exception {
        String topicName = "test-topic-" + rng.nextInt(Integer.MAX_VALUE);

        SecretUtils.createCustomSecret(CLUSTER_NAME, NAMESPACE,
                getClass().getClassLoader().getResource("custom-certs/strimzi-bundle.crt").getFile(),
                getClass().getClassLoader().getResource("custom-certs/strimzi-key.pem").getFile());

        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3, 3)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewKafkaListenerExternalLoadBalancer()
                        .endKafkaListenerExternalLoadBalancer()
                    .endListeners()
                    .withConfig(singletonMap("default.replication.factor", 3))
                .endKafka()
            .endSpec().done();

        String userName = "alice";
        KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();

        sendMessagesExternalTls(NAMESPACE, topicName, 10, userName);
        receiveMessagesExternalTls(NAMESPACE, topicName, 10, userName, "consumer-group-certs-2");

        Map<String, String> kafkaSnapshot = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            kafka.getSpec().getKafka().getListeners().setExternal(new KafkaListenerExternalNodePortBuilder()
                .withNewConfiguration()
                    .withNewBrokerCertChainAndKey()
                        .withSecretName("custom-certificate")
                        .withKey("ca.key")
                        .withCertificate("ca.crt")
                    .endBrokerCertChainAndKey()
                .endConfiguration()
                .build());
        });

        kafkaSnapshot = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3);

        sendMessagesExternalTls(NAMESPACE, topicName, 10, "", "custom-certificate");
        receiveMessagesExternalTls(NAMESPACE, topicName, 20, "", "consumer-group-certs-66", "custom-certificate");

        SecretUtils.createCustomSecret(CLUSTER_NAME, NAMESPACE,
                getClass().getClassLoader().getResource("custom-certs/strimzi-bundle-2.crt").getFile(),
                getClass().getClassLoader().getResource("custom-certs/strimzi-key-2.pem").getFile());

        kafkaSnapshot = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3);

        sendMessagesExternalTls(NAMESPACE, topicName, 10, "", "custom-certificate");
        receiveMessagesExternalTls(NAMESPACE, topicName, 30, "", "consumer-group-certs-68", "custom-certificate");

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            kafka.getSpec().getKafka().getListeners().setExternal(new KafkaListenerExternalNodePortBuilder()
                .withTls(true)
                .build());
        });

        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3);

        sendMessagesExternalTls(NAMESPACE, topicName, 10, userName);
        receiveMessagesExternalTls(NAMESPACE, topicName, 40, userName, "consumer-group-certs-92");
    }

    @Test
    @OpenShiftOnly
    void testCustomCertRouteRollingUpdate() throws Exception {
        String topicName = "test-topic-" + rng.nextInt(Integer.MAX_VALUE);

        SecretUtils.createCustomSecret(CLUSTER_NAME, NAMESPACE,
                getClass().getClassLoader().getResource("custom-certs/strimzi-bundle.crt").getFile(),
                getClass().getClassLoader().getResource("custom-certs/strimzi-key.pem").getFile());

        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3, 3)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewKafkaListenerExternalNodePort()
                        .endKafkaListenerExternalNodePort()
                    .endListeners()
                    .withConfig(singletonMap("default.replication.factor", 3))
                .endKafka()
            .endSpec().done();

        String userName = "alice";
        KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();

        sendMessagesExternalTls(NAMESPACE, topicName, 10, userName);
        receiveMessagesExternalTls(NAMESPACE, topicName, 10, userName, "consumer-group-certs-2");

        Map<String, String> kafkaSnapshot = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            kafka.getSpec().getKafka().getListeners().setExternal(new KafkaListenerExternalNodePortBuilder()
                .withNewConfiguration()
                    .withNewBrokerCertChainAndKey()
                        .withSecretName("custom-certificate")
                        .withKey("ca.key")
                        .withCertificate("ca.crt")
                    .endBrokerCertChainAndKey()
                .endConfiguration()
                .build());
        });

        kafkaSnapshot = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3);

        sendMessagesExternalTls(NAMESPACE, topicName, 10, "", "custom-certificate");
        receiveMessagesExternalTls(NAMESPACE, topicName, 20, "", "consumer-group-certs-66", "custom-certificate");

        SecretUtils.createCustomSecret(CLUSTER_NAME, NAMESPACE,
                getClass().getClassLoader().getResource("custom-certs/strimzi-bundle-2.crt").getFile(),
                getClass().getClassLoader().getResource("custom-certs/strimzi-key-2.pem").getFile());

        kafkaSnapshot = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3);

        sendMessagesExternalTls(NAMESPACE, topicName, 10, "", "custom-certificate");
        receiveMessagesExternalTls(NAMESPACE, topicName, 30, "", "consumer-group-certs-68", "custom-certificate");

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            kafka.getSpec().getKafka().getListeners().setExternal(new KafkaListenerExternalNodePortBuilder()
                .withTls(true)
                .build());
        });

        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3);

        sendMessagesExternalTls(NAMESPACE, topicName, 10, userName);
        receiveMessagesExternalTls(NAMESPACE, topicName, 40, userName, "consumer-group-certs-92");
    }

    @BeforeAll
    void setup() {
        ResourceManager.setClassResources();

        prepareEnvForOperator(NAMESPACE);

        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        KubernetesResource.clusterOperator(NAMESPACE).done();
        cluster.setNamespace(NAMESPACE);
    }

    @Override
    protected void tearDownEnvironmentAfterEach() throws Exception {
        super.tearDownEnvironmentAfterEach();
        kubeClient().getClient().persistentVolumeClaims().inNamespace(NAMESPACE).delete();
    }
}
