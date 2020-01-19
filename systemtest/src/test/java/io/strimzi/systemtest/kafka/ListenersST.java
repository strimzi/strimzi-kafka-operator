/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.listener.KafkaListenerExternalNodePortBuilder;
import io.strimzi.api.kafka.model.listener.KafkaListenerTlsBuilder;
import io.strimzi.systemtest.BaseST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
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
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
public class ListenersST extends BaseST {
    private static final Logger LOGGER = LogManager.getLogger(ListenersST.class);

    public static final String NAMESPACE = "custom-certs-cluster-test";

    @Test
    @Tag(NODEPORT_SUPPORTED)
    void testCustomSoloCertificatesForNodePort() throws Exception {
        String topicName = "test-topic-" + rng.nextInt(Integer.MAX_VALUE);

        SecretUtils.createCustomSecret("custom-certificate", CLUSTER_NAME, NAMESPACE,
                getClass().getClassLoader().getResource("custom-certs/strimzi.pem").getFile(),
                getClass().getClassLoader().getResource("custom-certs/strimzi.key").getFile());

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 3)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewTls()
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName("custom-certificate")
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .endTls()
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

        String userName = "alice";
        KafkaUser aliceUser = KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();

        kafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "SSL");
        kafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "SSL");

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).done();

        externalKafkaClient.setPodName(kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName());
        int sent = externalKafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "TLS");
        assertThat(sent, is(10));
        int received = externalKafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 20, "TLS", "consumer-group-certs-1");
        assertThat(received, is(20));
    }

    @Test
    @Tag(NODEPORT_SUPPORTED)
    void testCustomChainCertificatesForNodePort() throws Exception {
        String topicName = "test-topic-" + rng.nextInt(Integer.MAX_VALUE);

        SecretUtils.createCustomSecret("custom-certificate", CLUSTER_NAME, NAMESPACE,
                getClass().getClassLoader().getResource("custom-certs/strimzi-bundle.crt").getFile(),
                getClass().getClassLoader().getResource("custom-certs/strimzi-key.pem").getFile());

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 3)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewTls()
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName("custom-certificate")
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .endTls()
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

        String userName = "alice";
        KafkaUser aliceUser = KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();

        kafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "SSL");
        kafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "SSL");

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).done();

        String defaultKafkaClientsPodName =
                kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        externalKafkaClient.setPodName(kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName());
        int sent = externalKafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "TLS");
        assertThat(sent, is(10));
        int received = externalKafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 20, "TLS", "consumer-group-certs-2");
        assertThat(received, is(20));
    }

    @Test
    @Tag(LOADBALANCER_SUPPORTED)
    void testCustomSoloCertificatesForLoadBalancer() throws Exception {
        String topicName = "test-topic-" + rng.nextInt(Integer.MAX_VALUE);

        SecretUtils.createCustomSecret("custom-certificate", CLUSTER_NAME, NAMESPACE,
                getClass().getClassLoader().getResource("custom-certs/strimzi.pem").getFile(),
                getClass().getClassLoader().getResource("custom-certs/strimzi.key").getFile());

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 3)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewTls()
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName("custom-certificate")
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .endTls()
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

        String userName = "alice";
        KafkaUser aliceUser = KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();

        kafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "SSL");
        kafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "SSL");

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).done();

        String defaultKafkaClientsPodName =
                kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        externalKafkaClient.setPodName(kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName());
        int sent = externalKafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "TLS");
        assertThat(sent, is(10));
        int received = externalKafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 20, "TLS", "consumer-group-certs-3");
        assertThat(received, is(20));
    }

    @Test
    @Tag(LOADBALANCER_SUPPORTED)
    void testCustomChainCertificatesForLoadBalancer() throws Exception {
        String topicName = "test-topic-" + rng.nextInt(Integer.MAX_VALUE);

        SecretUtils.createCustomSecret("custom-certificate", CLUSTER_NAME, NAMESPACE,
                getClass().getClassLoader().getResource("custom-certs/strimzi-bundle.crt").getFile(),
                getClass().getClassLoader().getResource("custom-certs/strimzi-key.pem").getFile());

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 3)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewTls()
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName("custom-certificate")
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .endTls()
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

        String userName = "alice";
        KafkaUser aliceUser = KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();

        kafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "SSL");
        kafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "SSL");

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).done();

        externalKafkaClient.setPodName(kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName());
        int sent = externalKafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "TLS");
        assertThat(sent, is(10));
        int received = externalKafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 20, "TLS", "consumer-group-certs-4");
        assertThat(received, is(20));
    }

    @Test
    @OpenShiftOnly
    void testCustomSoloCertificatesForRoute() throws Exception {
        String topicName = "test-topic-" + rng.nextInt(Integer.MAX_VALUE);

        SecretUtils.createCustomSecret("custom-certificate", CLUSTER_NAME, NAMESPACE,
                getClass().getClassLoader().getResource("custom-certs/strimzi.pem").getFile(),
                getClass().getClassLoader().getResource("custom-certs/strimzi.key").getFile());

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 3)
            .editSpec()
                .editKafka()
                    .editListeners()
                         .withNewTls()
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName("custom-certificate")
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .endTls()
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

        String userName = "alice";
        KafkaUser aliceUser = KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();

        kafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "SSL");
        kafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "SSL");

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).done();

        externalKafkaClient.setPodName(kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName());
        int sent = externalKafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "TLS");
        assertThat(sent, is(10));
        int received = externalKafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 20, "TLS", "consumer-group-certs-5");
        assertThat(received, is(20));
    }

    @Test
    @OpenShiftOnly
    void testCustomChainCertificatesForRoute() throws Exception {
        String topicName = "test-topic-" + rng.nextInt(Integer.MAX_VALUE);
        LOGGER.info(kubeClient().getClient().getConfiguration());

        SecretUtils.createCustomSecret("custom-certificate", CLUSTER_NAME, NAMESPACE,
                getClass().getClassLoader().getResource("custom-certs/strimzi-bundle.crt").getFile(),
                getClass().getClassLoader().getResource("custom-certs/strimzi-key.pem").getFile());

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 3)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewTls()
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName("custom-certificate")
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .endTls()
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

        String userName = "alice";
        KafkaUser aliceUser = KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();

        kafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "SSL");
        kafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "SSL");

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).done();

        externalKafkaClient.setPodName(kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName());
        int sent = externalKafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "TLS");
        assertThat(sent, is(10));
        int received = externalKafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 20, "TLS", "consumer-group-certs-6");
        assertThat(received, is(20));
    }


    @Test
    @Tag(NODEPORT_SUPPORTED)
    void testCustomCertNodePortAndTlsRollingUpdate() throws Exception {
        String topicName = "test-topic-" + rng.nextInt(Integer.MAX_VALUE);

        SecretUtils.createCustomSecret("custom-certificate-1", CLUSTER_NAME, NAMESPACE,
                getClass().getClassLoader().getResource("custom-certs/strimzi-bundle.crt").getFile(),
                getClass().getClassLoader().getResource("custom-certs/strimzi-key.pem").getFile());

        SecretUtils.createCustomSecret("custom-certificate-2", CLUSTER_NAME, NAMESPACE,
                getClass().getClassLoader().getResource("custom-certs/strimzi-bundle-2.crt").getFile(),
                getClass().getClassLoader().getResource("custom-certs/strimzi-key-2.pem").getFile());

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
        KafkaUser aliceUser = KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();

        kafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "SSL");
        kafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "SSL");

        Map<String, String> kafkaSnapshot = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            kafka.getSpec().getKafka().getListeners().setExternal(new KafkaListenerExternalNodePortBuilder()
                .withNewConfiguration()
                    .withNewBrokerCertChainAndKey()
                        .withSecretName("custom-certificate-1")
                        .withKey("ca.key")
                        .withCertificate("ca.crt")
                    .endBrokerCertChainAndKey()
                .endConfiguration()
                .build());
            kafka.getSpec().getKafka().getListeners().setTls(new KafkaListenerTlsBuilder()
                .withNewConfiguration()
                    .withNewBrokerCertChainAndKey()
                        .withSecretName("custom-certificate-2")
                        .withKey("ca.key")
                        .withCertificate("ca.crt")
                    .endBrokerCertChainAndKey()
                .endConfiguration()
                .build());
        });

        kafkaSnapshot = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3);

        kafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "SSL");
        kafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 20, "SSL");

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).done();

        externalKafkaClient.setPodName(kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName());
        int sent = externalKafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "TLS");
        assertThat(sent, is(10));
        int received = externalKafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 30, "TLS", "consumer-group-certs-7");
        assertThat(received, is(30));


        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            kafka.getSpec().getKafka().getListeners().setExternal(new KafkaListenerExternalNodePortBuilder()
                .withTls(true)
                .build());
        });

        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3);

        kafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "SSL");
        kafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 40, "SSL");

        received = externalKafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 40, "TLS", "consumer-group-certs-72");
        assertThat(received, is(40));
    }

    @Test
    @Tag(LOADBALANCER_SUPPORTED)
    void testCustomCertLoadBalancerAndTlsRollingUpdate() throws Exception {
        String topicName = "test-topic-" + rng.nextInt(Integer.MAX_VALUE);

        SecretUtils.createCustomSecret("custom-certificate-1", CLUSTER_NAME, NAMESPACE,
                getClass().getClassLoader().getResource("custom-certs/strimzi-bundle.crt").getFile(),
                getClass().getClassLoader().getResource("custom-certs/strimzi-key.pem").getFile());

        SecretUtils.createCustomSecret("custom-certificate-2", CLUSTER_NAME, NAMESPACE,
                getClass().getClassLoader().getResource("custom-certs/strimzi-bundle-2.crt").getFile(),
                getClass().getClassLoader().getResource("custom-certs/strimzi-key-2.pem").getFile());

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
        KafkaUser aliceUser = KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();

        kafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "SSL");
        kafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "SSL");

        Map<String, String> kafkaSnapshot = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            kafka.getSpec().getKafka().getListeners().setExternal(new KafkaListenerExternalNodePortBuilder()
                .withNewConfiguration()
                    .withNewBrokerCertChainAndKey()
                        .withSecretName("custom-certificate-1")
                        .withKey("ca.key")
                        .withCertificate("ca.crt")
                    .endBrokerCertChainAndKey()
                .endConfiguration()
                .build());
            kafka.getSpec().getKafka().getListeners().setTls(new KafkaListenerTlsBuilder()
                .withNewConfiguration()
                    .withNewBrokerCertChainAndKey()
                        .withSecretName("custom-certificate-2")
                        .withKey("ca.key")
                        .withCertificate("ca.crt")
                    .endBrokerCertChainAndKey()
                .endConfiguration()
                .build());
        });

        kafkaSnapshot = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3);

        kafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "SSL");
        kafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 20, "SSL");

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).done();

        externalKafkaClient.setPodName(kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName());
        int sent = externalKafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "TLS");
        assertThat(sent, is(10));
        int received = externalKafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 30, "TLS", "consumer-group-certs-81");
        assertThat(received, is(30));

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            kafka.getSpec().getKafka().getListeners().setExternal(new KafkaListenerExternalNodePortBuilder()
                .withTls(true)
                .build());
        });

        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3);

        kafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "SSL");
        kafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 40, "SSL");

        received = externalKafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 40, "TLS", "consumer-group-certs-82");
        assertThat(received, is(40));
    }

    @Test
    @OpenShiftOnly
    void testCustomCertRouteAndTlsRollingUpdate() throws Exception {
        String topicName = "test-topic-" + rng.nextInt(Integer.MAX_VALUE);

        SecretUtils.createCustomSecret("custom-certificate-1", CLUSTER_NAME, NAMESPACE,
                getClass().getClassLoader().getResource("custom-certs/strimzi-bundle.crt").getFile(),
                getClass().getClassLoader().getResource("custom-certs/strimzi-key.pem").getFile());

        SecretUtils.createCustomSecret("custom-certificate-2", CLUSTER_NAME, NAMESPACE,
                getClass().getClassLoader().getResource("custom-certs/strimzi-bundle-2.crt").getFile(),
                getClass().getClassLoader().getResource("custom-certs/strimzi-key-2.pem").getFile());

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
        KafkaUser aliceUser = KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();

        kafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "SSL");
        kafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "SSL");

        Map<String, String> kafkaSnapshot = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            kafka.getSpec().getKafka().getListeners().setExternal(new KafkaListenerExternalNodePortBuilder()
                .withNewConfiguration()
                    .withNewBrokerCertChainAndKey()
                        .withSecretName("custom-certificate-1")
                        .withKey("ca.key")
                        .withCertificate("ca.crt")
                    .endBrokerCertChainAndKey()
                .endConfiguration()
                .build());
            kafka.getSpec().getKafka().getListeners().setTls(new KafkaListenerTlsBuilder()
                .withNewConfiguration()
                    .withNewBrokerCertChainAndKey()
                        .withSecretName("custom-certificate-2")
                        .withKey("ca.key")
                        .withCertificate("ca.crt")
                    .endBrokerCertChainAndKey()
                .endConfiguration()
                .build());
        });

        kafkaSnapshot = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3);

        kafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "SSL");
        kafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 20, "SSL");

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).done();

        externalKafkaClient.setPodName(kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName());
        int sent = externalKafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "TLS");
        assertThat(sent, is(10));
        int received = externalKafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 30, "TLS", "consumer-group-certs-91");
        assertThat(received, is(30));

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            kafka.getSpec().getKafka().getListeners().setExternal(new KafkaListenerExternalNodePortBuilder()
                .withTls(true)
                .build());
        });

        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3);

        kafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 10, "SSL");
        kafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 40, "SSL");

        received = externalKafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, 40, "TLS", "consumer-group-certs-92");
        assertThat(received, is(40));
    }

    @Test
    void testNonExistingCustomCertificate() throws Exception {
        String nonExistingCertName = "non-existing-certificate";
        SecretUtils.createCustomSecret("custom-certificate", CLUSTER_NAME, NAMESPACE,
                getClass().getClassLoader().getResource("custom-certs/strimzi.pem").getFile(),
                getClass().getClassLoader().getResource("custom-certs/strimzi.key").getFile());

        KafkaResource.kafkaWithoutWait(KafkaResource.defaultKafka(CLUSTER_NAME, 1, 1)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewTls()
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(nonExistingCertName)
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .endTls()
                    .endListeners()
                .endKafka()
            .endSpec().build());

        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(CLUSTER_NAME, NAMESPACE, "Secret " + nonExistingCertName + ".*does not exist.*");
    }

    @Test
    void testCertificateWithNonExistingDataCrt() {
        String certName = "custom-certificate";
        String nonExistingCertName = "non-existing-crt";
        SecretUtils.createCustomSecret(certName, CLUSTER_NAME, NAMESPACE,
                getClass().getClassLoader().getResource("custom-certs/strimzi.pem").getFile(),
                getClass().getClassLoader().getResource("custom-certs/strimzi.key").getFile());

        KafkaResource.kafkaWithoutWait(KafkaResource.defaultKafka(CLUSTER_NAME, 1, 1)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewTls()
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(certName)
                                    .withKey("ca.key")
                                    .withCertificate(nonExistingCertName)
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .endTls()
                    .endListeners()
                .endKafka()
            .endSpec().build());

        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(CLUSTER_NAME, NAMESPACE,
                "Secret " + certName + ".*does not contain certificate under the key " + nonExistingCertName + ".*");
    }

    @Test
    void testCertificateWithNonExistingDataKey() {
        String certName = "custom-certificate";
        String nonExistingCertKey = "non-existing-key";
        SecretUtils.createCustomSecret(certName, CLUSTER_NAME, NAMESPACE,
                getClass().getClassLoader().getResource("custom-certs/strimzi.pem").getFile(),
                getClass().getClassLoader().getResource("custom-certs/strimzi.key").getFile());

        KafkaResource.kafkaWithoutWait(KafkaResource.defaultKafka(CLUSTER_NAME, 1, 1)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewTls()
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(certName)
                                    .withKey(nonExistingCertKey)
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .endTls()
                    .endListeners()
                .endKafka()
            .endSpec().build());

        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(CLUSTER_NAME, NAMESPACE,
                "Secret " + certName + ".*does not contain.*private key under the key " + nonExistingCertKey + ".*");
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
