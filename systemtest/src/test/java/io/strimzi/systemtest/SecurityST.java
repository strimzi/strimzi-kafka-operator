/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyPeerBuilder;
import io.strimzi.api.kafka.model.AclOperation;
import io.strimzi.api.kafka.model.CertificateAuthorityBuilder;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.status.KafkaConnectStatus;
import io.strimzi.api.kafka.model.status.KafkaMirrorMakerStatus;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.kafkaclients.ClientFactory;
import io.strimzi.systemtest.kafkaclients.EClientType;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMakerResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.systemtest.utils.specific.MetricsUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.strimzi.api.kafka.model.KafkaResources.clientsCaCertificateSecretName;
import static io.strimzi.api.kafka.model.KafkaResources.clientsCaKeySecretName;
import static io.strimzi.api.kafka.model.KafkaResources.clusterCaCertificateSecretName;
import static io.strimzi.api.kafka.model.KafkaResources.clusterCaKeySecretName;
import static io.strimzi.api.kafka.model.KafkaResources.kafkaStatefulSetName;
import static io.strimzi.api.kafka.model.KafkaResources.zookeeperStatefulSetName;
import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.NETWORKPOLICIES_SUPPORTED;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.TestUtils.map;
import static io.strimzi.test.TestUtils.waitFor;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(REGRESSION)
class SecurityST extends BaseST {

    public static final String NAMESPACE = "security-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(SecurityST.class);
    private static final String OPENSSL_RETURN_CODE = "Verify return code: 0 (ok)";
    private static final String TLS_PROTOCOL = "Protocol  : TLSv1";
    private static final String SSL_TIMEOUT = "Timeout   : 300 (sec)";
    private static final String TOPIC_NAME = "test-topic";

    private InternalKafkaClient internalKafkaClient = (InternalKafkaClient) ClientFactory.getClient(EClientType.INTERNAL);
    private int messagesCount = 200;

    @Test
    void testCertificates() {
        LOGGER.info("Running testCertificates {}", CLUSTER_NAME);
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 2)
                .editSpec().editZookeeper().withReplicas(2).endZookeeper().endSpec().done();
        String commandForKafkaBootstrap = "echo -n | openssl s_client -connect my-cluster-kafka-bootstrap:9093 -showcerts" +
                " -CAfile /opt/kafka/cluster-ca-certs/ca.crt" +
                " -verify_hostname my-cluster-kafka-bootstrap";

        String outputForKafkaBootstrap =
                cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0),
                        "/bin/bash", "-c", commandForKafkaBootstrap).out();
        checkKafkaCertificates(outputForKafkaBootstrap);

        String commandForZookeeperClient = "echo -n | openssl s_client -connect my-cluster-zookeeper-client:2181 -showcerts" +
                " -CAfile /opt/kafka/cluster-ca-certs/ca.crt" +
                " -verify_hostname my-cluster-zookeeper-client" +
                " -cert /opt/kafka/broker-certs/my-cluster-kafka-0.crt" +
                " -key /opt/kafka/broker-certs/my-cluster-kafka-0.key";
        String outputForZookeeperClient =
                cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0),
                        "/bin/bash", "-c", commandForZookeeperClient).out();
        checkZookeeperCertificates(outputForZookeeperClient);

        IntStream.rangeClosed(0, 1).forEach(podId -> {
            String commandForKafkaPort9091 = "echo -n | " + generateOpenSSLCommandWithCerts(KafkaResources.kafkaPodName(CLUSTER_NAME, podId), "my-cluster-kafka-brokers", "9091");
            String commandForKafkaPort9093 = "echo -n | " + generateOpenSSLCommandWithCAfile(KafkaResources.kafkaPodName(CLUSTER_NAME, podId), "my-cluster-kafka-brokers", "9093");

            String outputForKafkaPort9091 =
                    cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, podId), "/bin/bash", "-c", commandForKafkaPort9091).out();
            String outputForKafkaPort9093 =
                    cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, podId), "/bin/bash", "-c", commandForKafkaPort9093).out();

            checkKafkaCertificates(outputForKafkaPort9091, outputForKafkaPort9093);

            String commandForZookeeperPort2181 = "echo -n | " + generateOpenSSLCommandWithCerts(KafkaResources.zookeeperPodName(CLUSTER_NAME, podId), "my-cluster-zookeeper-nodes", "2181");
            String commandForZookeeperPort2888 = "echo -n | " + generateOpenSSLCommandWithCerts(KafkaResources.zookeeperPodName(CLUSTER_NAME, podId), "my-cluster-zookeeper-nodes", "2888");
            String commandForZookeeperPort3888 = "echo -n | " + generateOpenSSLCommandWithCerts(KafkaResources.zookeeperPodName(CLUSTER_NAME, podId), "my-cluster-zookeeper-nodes", "3888");

            String outputForZookeeperPort2181 =
                    cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, podId), "/bin/bash", "-c",
                            commandForZookeeperPort2181).out();

            String outputForZookeeperPort3888 =
                    cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, podId), "/bin/bash", "-c",
                            commandForZookeeperPort3888).out();
            checkZookeeperCertificates(outputForZookeeperPort2181, outputForZookeeperPort3888);

            try {
                String outputForZookeeperPort2888 =
                        cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, podId), "/bin/bash", "-c",
                                commandForZookeeperPort2888).out();
                checkZookeeperCertificates(outputForZookeeperPort2888);
            } catch (KubeClusterException e) {
                if (e.result != null && e.result.returnCode() == 104) {
                    LOGGER.info("The connection for {} was forcibly closed because of new zookeeper leader", KafkaResources.zookeeperPodName(CLUSTER_NAME, podId));
                } else {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private String generateOpenSSLCommandWithCAfile(String podName, String hostname, String port) {
        return "openssl s_client -connect " + podName + "." + hostname + ":" + port +
                " -showcerts -CAfile /opt/kafka/cluster-ca-certs/ca.crt " +
                "-verify_hostname " + podName + "." + hostname + "." + NAMESPACE + ".svc.cluster.local";
    }

    private String generateOpenSSLCommandWithCerts(String podName, String hostname, String port) {
        return generateOpenSSLCommandWithCAfile(podName, hostname, port) +
                " -cert /opt/kafka/broker-certs/my-cluster-kafka-0.crt" +
                " -key /opt/kafka/broker-certs/my-cluster-kafka-0.key";
    }

    private void checkKafkaCertificates(String... kafkaCertificates) {
        String kafkaCertificateChain = "s:/O=io.strimzi/CN=my-cluster-kafka\n" +
                "   i:/O=io.strimzi/CN=cluster-ca";
        String kafkaBrokerCertificate = "Server certificate\n" +
                "subject=/O=io.strimzi/CN=my-cluster-kafka\n" +
                "issuer=/O=io.strimzi/CN=cluster-ca";
        for (String kafkaCertificate : kafkaCertificates) {
            assertThat(kafkaCertificate, containsString(kafkaCertificateChain));
            assertThat(kafkaCertificate, containsString(kafkaBrokerCertificate));
            assertThat(kafkaCertificate, containsString(TLS_PROTOCOL));
            assertThat(kafkaCertificate, containsString(SSL_TIMEOUT));
            assertThat(kafkaCertificate, containsString(OPENSSL_RETURN_CODE));
        }
    }

    private void checkZookeeperCertificates(String... zookeeperCertificates) {
        String zookeeperCertificateChain = "s:/O=io.strimzi/CN=my-cluster-zookeeper\n" +
                "   i:/O=io.strimzi/CN=cluster-ca";
        String zookeeperNodeCertificate = "Server certificate\n" +
                "subject=/O=io.strimzi/CN=my-cluster-zookeeper\n" +
                "issuer=/O=io.strimzi/CN=cluster-ca";
        for (String zookeeperCertificate : zookeeperCertificates) {
            assertThat(zookeeperCertificate, containsString(zookeeperCertificateChain));
            assertThat(zookeeperCertificate, containsString(zookeeperNodeCertificate));
            assertThat(zookeeperCertificate, containsString(TLS_PROTOCOL));
            assertThat(zookeeperCertificate, containsString(SSL_TIMEOUT));
            assertThat(zookeeperCertificate, containsString(OPENSSL_RETURN_CODE));
        }
    }

    void autoRenewSomeCaCertsTriggeredByAnno(
            final List<String> secretsToAnnotate,
            boolean zkShouldRoll,
            boolean kafkaShouldRoll,
            boolean eoShouldRoll) {
        String userName = "alice";
        String topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);

        createKafkaCluster();

        KafkaUser user = KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();
        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, user).done();

        SecretUtils.waitForSecretReady(userName);

        String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        internalKafkaClient.setPodName(defaultKafkaClientsPodName);

        LOGGER.info("Checking produced and consumed messages to pod:{}", internalKafkaClient.getPodName());
        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, messagesCount, "TLS"),
            internalKafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, messagesCount, "TLS", CONSUMER_GROUP_NAME)
        );

        // Get all pods, and their resource versions
        Map<String, String> zkPods = StatefulSetUtils.ssSnapshot(zookeeperStatefulSetName(CLUSTER_NAME));
        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(CLUSTER_NAME));
        Map<String, String> eoPod = DeploymentUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME));

        LOGGER.info("Triggering CA cert renewal by adding the annotation");
        Map<String, String> initialCaCerts = new HashMap<>();
        for (String secretName : secretsToAnnotate) {
            Secret secret = kubeClient().getSecret(secretName);
            String value = secret.getData().get("ca.crt");
            assertThat("ca.crt in " + secretName + " should not be null", value, is(notNullValue()));
            initialCaCerts.put(secretName, value);
            Secret annotated = new SecretBuilder(secret)
                    .editMetadata()
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_FORCE_RENEW, "true")
                    .endMetadata()
                    .build();
            LOGGER.info("Patching secret {} with {}", secretName, Ca.ANNO_STRIMZI_IO_FORCE_RENEW);
            kubeClient().patchSecret(secretName, annotated);
        }

        if (zkShouldRoll) {
            LOGGER.info("Wait for zk to rolling restart ...");
            StatefulSetUtils.waitTillSsHasRolled(zookeeperStatefulSetName(CLUSTER_NAME), 3, zkPods);
        }
        if (kafkaShouldRoll) {
            LOGGER.info("Wait for kafka to rolling restart ...");
            StatefulSetUtils.waitTillSsHasRolled(kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);
        }
        if (eoShouldRoll) {
            LOGGER.info("Wait for EO to rolling restart ...");
            eoPod = DeploymentUtils.waitTillDepHasRolled(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), 1, eoPod);
        }

        LOGGER.info("Checking the certificates have been replaced");
        for (String secretName : secretsToAnnotate) {
            Secret secret = kubeClient().getSecret(secretName);
            assertThat("Secret " + secretName + " should exist", secret, is(notNullValue()));
            assertThat("CA cert in " + secretName + " should have non-null 'data'", is(notNullValue()));
            String value = secret.getData().get("ca.crt");
            assertThat("CA cert in " + secretName + " should have changed",
                    value, is(not(initialCaCerts.get(secretName))));
        }

        LOGGER.info("Checking consumed messages to pod:{}", internalKafkaClient.getPodName());
        internalKafkaClient.checkProducedAndConsumedMessages(
            messagesCount,
            internalKafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, messagesCount, "TLS", CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE))
        );

        // Check a new client (signed by new client key) can consume
        String bobUserName = "bob";
        user = KafkaUserResource.tlsUser(CLUSTER_NAME, bobUserName).done();
        SecretUtils.waitForSecretReady(bobUserName);
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, user).done();

        defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        internalKafkaClient.setPodName(defaultKafkaClientsPodName);

        LOGGER.info("Checking consumed messages to pod:{}", internalKafkaClient.getPodName());
        internalKafkaClient.checkProducedAndConsumedMessages(
            messagesCount,
            internalKafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, bobUserName, messagesCount, "TLS", CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE))
        );

        if (!zkShouldRoll) {
            assertThat("ZK pods should not roll, but did.", StatefulSetUtils.ssSnapshot(zookeeperStatefulSetName(CLUSTER_NAME)), is(zkPods));

        }
        if (!kafkaShouldRoll) {
            assertThat("Kafka pods should not roll, but did.", StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(CLUSTER_NAME)), is(kafkaPods));

        }
        if (!eoShouldRoll) {
            assertThat("EO pod should not roll, but did.", DeploymentUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME)), is(eoPod));
        }
    }

    @Test
    void testAutoRenewClusterCaCertsTriggeredByAnno() throws Exception {
        autoRenewSomeCaCertsTriggeredByAnno(asList(
                clusterCaCertificateSecretName(CLUSTER_NAME)),
                /* ZK node need new certs */
                true,
                /* brokers need new certs */
                true,
                /* eo needs new cert */
                true);
    }

    @Test
    void testAutoRenewClientsCaCertsTriggeredByAnno() throws Exception {
        autoRenewSomeCaCertsTriggeredByAnno(asList(
                clientsCaCertificateSecretName(CLUSTER_NAME)),
                /* no communication between clients and zk, so no need to roll */
                false,
                /* brokers need to trust client certs with new cert */
                true,
                /* eo needs to generate new client certs */
                true);
    }

    @Test
    @Tag(ACCEPTANCE)
    void testAutoRenewAllCaCertsTriggeredByAnno() throws Exception {
        autoRenewSomeCaCertsTriggeredByAnno(asList(
                clusterCaCertificateSecretName(CLUSTER_NAME),
                clientsCaCertificateSecretName(CLUSTER_NAME)),
                true,
                true,
                true);
    }

    void autoReplaceSomeKeysTriggeredByAnno(final List<String> secrets,
                                            boolean zkShouldRoll,
                                            boolean kafkaShouldRoll,
                                            boolean eoShouldRoll) throws Exception {
        createKafkaCluster();

        String aliceUserName = "alice";
        KafkaUser user = KafkaUserResource.tlsUser(CLUSTER_NAME, aliceUserName).done();
        String topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);

        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, user).done();

        SecretUtils.waitForSecretReady(aliceUserName);

        String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        internalKafkaClient.setPodName(defaultKafkaClientsPodName);

        LOGGER.info("Checking produced and consumed messages to pod:{}", internalKafkaClient.getPodName());
        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, aliceUserName, messagesCount, "TLS"),
            internalKafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, aliceUserName, messagesCount, "TLS", CONSUMER_GROUP_NAME)
        );

        // Get all pods, and their resource versions
        Map<String, String> zkPods = StatefulSetUtils.ssSnapshot(zookeeperStatefulSetName(CLUSTER_NAME));
        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(CLUSTER_NAME));
        Map<String, String> eoPod = DeploymentUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME));

        LOGGER.info("Triggering CA cert renewal by adding the annotation");
        Map<String, String> initialCaKeys = new HashMap<>();
        for (String secretName : secrets) {
            Secret secret = kubeClient().getSecret(secretName);
            String value = secret.getData().get("ca.key");
            assertThat("ca.key in " + secretName + " should not be null", value, is(Matchers.notNullValue()));
            initialCaKeys.put(secretName, value);
            Secret annotated = new SecretBuilder(secret)
                    .editMetadata()
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_FORCE_REPLACE, "true")
                    .endMetadata()
                    .build();
            LOGGER.info("Patching secret {} with {}", secretName, Ca.ANNO_STRIMZI_IO_FORCE_REPLACE);
            kubeClient().patchSecret(secretName, annotated);
        }

        if (zkShouldRoll) {
            LOGGER.info("Wait for zk to rolling restart (1)...");
            zkPods = StatefulSetUtils.waitTillSsHasRolled(zookeeperStatefulSetName(CLUSTER_NAME), 3, zkPods);
        }
        if (kafkaShouldRoll) {
            LOGGER.info("Wait for kafka to rolling restart (1)...");
            kafkaPods = StatefulSetUtils.waitTillSsHasRolled(kafkaStatefulSetName(CLUSTER_NAME), kafkaPods);
        }
        if (eoShouldRoll) {
            LOGGER.info("Wait for EO to rolling restart (1)...");
            eoPod = DeploymentUtils.waitTillDepHasRolled(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), 1, eoPod);
        }

        if (zkShouldRoll) {
            LOGGER.info("Wait for zk to rolling restart (2)...");
            zkPods = StatefulSetUtils.waitTillSsHasRolled(zookeeperStatefulSetName(CLUSTER_NAME), 3, zkPods);
        }
        if (kafkaShouldRoll) {
            LOGGER.info("Wait for kafka to rolling restart (2)...");
            kafkaPods = StatefulSetUtils.waitTillSsHasRolled(kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);
        }
        if (eoShouldRoll) {
            LOGGER.info("Wait for EO to rolling restart (2)...");
            eoPod = DeploymentUtils.waitTillDepHasRolled(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), 1, eoPod);
        }

        LOGGER.info("Checking the certificates have been replaced");
        for (String secretName : secrets) {
            Secret secret = kubeClient().getSecret(secretName);
            assertThat("Secret " + secretName + " should exist", secret, is(notNullValue()));
            assertThat("CA key in " + secretName + " should have non-null 'data'", secret.getData(), is(notNullValue()));
            String value = secret.getData().get("ca.key");
            assertThat("CA key in " + secretName + " should exist", value, is(notNullValue()));
            assertThat("CA key in " + secretName + " should have changed",
                    value, is(not(initialCaKeys.get(secretName))));
        }

        LOGGER.info("Checking consumed messages to pod:{}", internalKafkaClient.getPodName());
        internalKafkaClient.checkProducedAndConsumedMessages(
            messagesCount,
            internalKafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, aliceUserName, messagesCount, "TLS", CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE))
        );

        // Finally check a new client (signed by new client key) can consume
        String bobUserName = "bob";
        user = KafkaUserResource.tlsUser(CLUSTER_NAME, bobUserName).done();
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, user).done();

        defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        internalKafkaClient.setPodName(defaultKafkaClientsPodName);

        LOGGER.info("Checking consumed messages to pod:{}", internalKafkaClient.getPodName());
        internalKafkaClient.checkProducedAndConsumedMessages(
            messagesCount,
            internalKafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, bobUserName, messagesCount, "TLS", CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE))
        );

        SecretUtils.waitForSecretReady(bobUserName);

        if (!zkShouldRoll) {
            assertThat("ZK pods should not roll, but did.", StatefulSetUtils.ssSnapshot(zookeeperStatefulSetName(CLUSTER_NAME)), is(zkPods));

        }
        if (!kafkaShouldRoll) {
            assertThat("Kafka pods should not roll, but did.", StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(CLUSTER_NAME)), is(kafkaPods));

        }
        if (!eoShouldRoll) {
            assertThat("EO pod should not roll, but did.", DeploymentUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME)), is(eoPod));
        }
    }

    @Test
    void testAutoReplaceClusterCaKeysTriggeredByAnno() throws Exception {
        autoReplaceSomeKeysTriggeredByAnno(asList(clusterCaKeySecretName(CLUSTER_NAME)),
                true,
                true,
                true);
    }

    @Test
    void testAutoReplaceClientsCaKeysTriggeredByAnno() throws Exception {
        autoReplaceSomeKeysTriggeredByAnno(asList(clientsCaKeySecretName(CLUSTER_NAME)),
                false,
                true,
                true);
    }

    @Test
    void testAutoReplaceAllCaKeysTriggeredByAnno() throws Exception {
        autoReplaceSomeKeysTriggeredByAnno(asList(clusterCaKeySecretName(CLUSTER_NAME),
                clientsCaKeySecretName(CLUSTER_NAME)),
                true,
                true,
                true);
    }

    private void createKafkaCluster() {
        LOGGER.info("Creating a cluster");
        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3)
                .editSpec()
                    .editKafka()
                        .editListeners()
                            .withNewTls()
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                            .endTls()
                        .endListeners()
                        .withConfig(singletonMap("default.replication.factor", 3))
                        .withNewPersistentClaimStorage()
                            .withSize("2Gi")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endKafka()
                    .editZookeeper()
                        .withNewPersistentClaimStorage()
                            .withSize("2Gi")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .done();

    }

    @Test
    void testAutoRenewCaCertsTriggerByExpiredCertificate() throws Exception {
        // 1. Create the Secrets already, and a certificate that's already expired
        String clusterCaCert = createSecret("cluster-ca.crt", clusterCaCertificateSecretName(CLUSTER_NAME), "ca.crt");
        String topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);

        // 2. Now create a cluster
        createKafkaCluster();
        String userName = "alice";
        KafkaUser user = KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();

        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, user).done();

        String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        internalKafkaClient.setPodName(defaultKafkaClientsPodName);

        // Check if user exists
        SecretUtils.waitForSecretReady(userName);

        LOGGER.info("Checking produced and consumed messages to pod:{}", internalKafkaClient.getPodName());
        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, messagesCount, "TLS"),
            internalKafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, messagesCount, "TLS", CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE))
        );

        // Wait until the certificates have been replaced
        waitForCertToChange(clusterCaCert, clusterCaCertificateSecretName(CLUSTER_NAME));

        // Wait until the pods are all up and ready
        waitForClusterStability();

        LOGGER.info("Checking produced and consumed messages to pod:{}", internalKafkaClient.getPodName());
        int received = internalKafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, messagesCount, "TLS", CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE));
        assertThat(received, is(messagesCount));

        topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);
        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, messagesCount, "TLS"),
            internalKafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, messagesCount, "TLS", CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE))
        );
    }

    @SuppressWarnings("unchecked")
    private void waitForClusterStability() {
        LOGGER.info("Waiting for cluster stability");
        Map<String, String>[] zkPods = new Map[1];
        Map<String, String>[] kafkaPods = new Map[1];
        Map<String, String>[] eoPods = new Map[1];
        AtomicInteger count = new AtomicInteger();
        zkPods[0] = StatefulSetUtils.ssSnapshot(zookeeperStatefulSetName(CLUSTER_NAME));
        kafkaPods[0] = StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(CLUSTER_NAME));
        eoPods[0] = DeploymentUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME));
        TestUtils.waitFor("Cluster stable and ready", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_CLUSTER_STABLE, () -> {
            Map<String, String> zkSnapshot = StatefulSetUtils.ssSnapshot(zookeeperStatefulSetName(CLUSTER_NAME));
            Map<String, String> kafkaSnaptop = StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(CLUSTER_NAME));
            Map<String, String> eoSnapshot = DeploymentUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME));
            boolean zkSameAsLast = zkSnapshot.equals(zkPods[0]);
            boolean kafkaSameAsLast = kafkaSnaptop.equals(kafkaPods[0]);
            boolean eoSameAsLast = eoSnapshot.equals(eoPods[0]);
            if (!zkSameAsLast) {
                LOGGER.info("ZK Cluster not stable");
            }
            if (!kafkaSameAsLast) {
                LOGGER.info("Kafka Cluster not stable");
            }
            if (!eoSameAsLast) {
                LOGGER.info("EO not stable");
            }
            if (zkSameAsLast
                    && kafkaSameAsLast
                    && eoSameAsLast) {
                int c = count.getAndIncrement();
                LOGGER.info("All stable for {} polls", c);
                return c > 60;
            }
            zkPods[0] = zkSnapshot;
            kafkaPods[0] = kafkaSnaptop;
            count.set(0);
            return false;
        });
    }

    private void waitForCertToChange(String originalCert, String secretName) {
        waitFor("Cert to be replaced", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_CLUSTER_STABLE, () -> {
            Secret secret = kubeClient().getSecret(secretName);
            if (secret != null && secret.getData() != null && secret.getData().containsKey("ca.crt")) {
                String currentCert = new String(Base64.getDecoder().decode(secret.getData().get("ca.crt")), StandardCharsets.US_ASCII);
                boolean changed = !originalCert.equals(currentCert);
                if (changed) {
                    LOGGER.info("Certificate in Secret {} has changed, was {}, is now {}", secretName, originalCert, currentCert);
                }
                return changed;
            } else {
                return false;
            }
        });
    }

    private String createSecret(String resourceName, String secretName, String keyName) {
        String certAsString = TestUtils.readResource(getClass(), resourceName);
        Secret secret = new SecretBuilder()
                .withNewMetadata()
                    .withName(secretName)
                    .addToLabels(map(
                        Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME,
                        Labels.STRIMZI_KIND_LABEL, "Kafka"))
                .endMetadata()
                .withData(singletonMap(keyName, Base64.getEncoder().encodeToString(certAsString.getBytes(StandardCharsets.US_ASCII))))
            .build();
        kubeClient().createSecret(secret);
        return certAsString;
    }

    @Test
    @Tag(NODEPORT_SUPPORTED)
    void testCertRenewalInMaintenanceWindow() {
        String secretName = CLUSTER_NAME + "-cluster-ca-cert";
        LocalDateTime maintenanceWindowStart = LocalDateTime.now().withSecond(0);
        long maintenanceWindowDuration = 14;
        maintenanceWindowStart = maintenanceWindowStart.plusMinutes(5);
        long windowStartMin = maintenanceWindowStart.getMinute();
        long windowStopMin = windowStartMin + maintenanceWindowDuration > 59
                ? windowStartMin + maintenanceWindowDuration - 60 : windowStartMin + maintenanceWindowDuration;

        String maintenanceWindowCron = "* " + windowStartMin + "-" + windowStopMin + " * * * ? *";
        LOGGER.info("Maintenance window is: {}", maintenanceWindowCron);
        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3, 1)
                .editSpec()
                    .addNewMaintenanceTimeWindow(maintenanceWindowCron)
                .endSpec().done();

        String aliceUserName = "alice";
        KafkaUser user = KafkaUserResource.tlsUser(CLUSTER_NAME, aliceUserName).done();
        String topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);

        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, user).done();

        String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        internalKafkaClient.setPodName(defaultKafkaClientsPodName);

        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(CLUSTER_NAME));

        LOGGER.info("Annotate secret {} with secret force-renew annotation", secretName);
        Secret secret = new SecretBuilder(kubeClient().getSecret(secretName))
            .editMetadata()
                .addToAnnotations(Ca.ANNO_STRIMZI_IO_FORCE_RENEW, "true")
            .endMetadata().build();
        kubeClient().patchSecret(secretName, secret);

        LOGGER.info("Wait until maintenance windows starts");
        LocalDateTime finalMaintenanceWindowStart = maintenanceWindowStart;
        TestUtils.waitFor("Wait until maintenance window start",
            Constants.GLOBAL_POLL_INTERVAL, Duration.ofMinutes(maintenanceWindowDuration).toMillis() - 10000,
            () -> LocalDateTime.now().isAfter(finalMaintenanceWindowStart));

        LOGGER.info("Maintenance window starts");

        assertThat("Rolling update was performed out of maintenance window!", kafkaPods, is(StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(CLUSTER_NAME))));

        LOGGER.info("Wait until rolling update is triggered during maintenance window");
        StatefulSetUtils.waitTillSsHasRolled(kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);

        assertThat("Rolling update wasn't performed in correct time", LocalDateTime.now().isAfter(maintenanceWindowStart));

        LOGGER.info("Checking produced and consumed messages to pod:{}", internalKafkaClient.getPodName());
        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, aliceUserName, messagesCount, "TLS"),
            internalKafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, aliceUserName, messagesCount, "TLS", CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE))
        );
    }

    @Test
    void testCertRegeneratedAfterInternalCAisDeleted() {
        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3, 1).done();

        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(CLUSTER_NAME));

        String userName = "user-example";
        KafkaUser user = KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();
        SecretUtils.waitForSecretReady(userName);

        String topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);

        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, user).done();

        String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        internalKafkaClient.setPodName(defaultKafkaClientsPodName);

        List<Secret> secrets = kubeClient().listSecrets().stream()
                .filter(secret -> secret.getMetadata().getName().endsWith("ca-cert"))
                .collect(Collectors.toList());

        for (Secret s : secrets) {
            LOGGER.info("Verifying that secret {} with name {} is present", s, s.getMetadata().getName());
            assertThat(s.getData(), is(notNullValue()));
        }

        for (Secret s : secrets) {
            LOGGER.info("Deleting secret {}", s.getMetadata().getName());
            kubeClient().deleteSecret(s.getMetadata().getName());
        }

        StUtils.waitForReconciliation(testClass, testName, NAMESPACE);
        StatefulSetUtils.waitTillSsHasRolled(kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);

        for (Secret s : secrets) {
            SecretUtils.waitForSecretReady(s.getMetadata().getName());
        }

        List<Secret> regeneratedSecrets = kubeClient().listSecrets().stream()
                .filter(secret -> secret.getMetadata().getName().endsWith("ca-cert"))
                .collect(Collectors.toList());

        for (int i = 0; i < secrets.size(); i++) {
            assertThat("Certificates has different cert UIDs", !secrets.get(i).getData().get("ca.crt").equals(regeneratedSecrets.get(i).getData().get("ca.crt")));
        }

        LOGGER.info("Checking produced and consumed messages to pod:{}", internalKafkaClient.getPodName());
        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, messagesCount, "TLS"),
            internalKafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, messagesCount, "TLS", CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE))
        );

    }

    @Test
    @Tag(NETWORKPOLICIES_SUPPORTED)
    void testNetworkPoliciesWithPlainListener() throws InterruptedException {
        Map<String, String> matchLabelForPlain = new HashMap<>();
        matchLabelForPlain.put("app", CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS);

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .withNewPlain()
                            .withNewKafkaListenerAuthenticationScramSha512Auth()
                            .endKafkaListenerAuthenticationScramSha512Auth()
                            .withNetworkPolicyPeers(
                                new NetworkPolicyPeerBuilder()
                                    .withNewPodSelector()
                                        .withMatchLabels(matchLabelForPlain)
                                    .endPodSelector()
                                    .build())
                            .endPlain()
                        .endListeners()
                    .endKafka()
                    .withNewKafkaExporter()
                    .endKafkaExporter()
                .endSpec()
            .done();

        String topic0 = "topic-example-0";
        String topic1 = "topic-example-1";

        String userName = "user-example";
        KafkaUser kafkaUser = KafkaUserResource.scramShaUser(CLUSTER_NAME, userName).done();
        SecretUtils.waitForSecretReady(userName);

        KafkaTopicResource.topic(CLUSTER_NAME, topic0).done();
        KafkaTopicResource.topic(CLUSTER_NAME, topic1).done();

        KafkaClientsResource.deployKafkaClients(false, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, kafkaUser).done();

        String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        LOGGER.info("Verifying that {} pod is able to exchange messages", kafkaClientsPodName);

        internalKafkaClient.setPodName(kafkaClientsPodName);

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(topic0, NAMESPACE, CLUSTER_NAME, userName, messagesCount, "TLS"),
            internalKafkaClient.receiveMessagesTls(topic0, NAMESPACE, CLUSTER_NAME, userName, messagesCount, "TLS", CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE))
        );

        KafkaClientsResource.deployKafkaClients(false, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS + "-new", kafkaUser).done();

        String kafkaClientsNewPodName = kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(1).getMetadata().getName();

        internalKafkaClient.setPodName(kafkaClientsNewPodName);

        LOGGER.info("Verifying that {} pod is not able to exchange messages", kafkaClientsNewPodName);
        assertThrows(AssertionError.class, () ->  {
            internalKafkaClient.checkProducedAndConsumedMessages(
                internalKafkaClient.sendMessagesTls(topic1, NAMESPACE, CLUSTER_NAME, userName, messagesCount, "TLS"),
                internalKafkaClient.receiveMessagesTls(topic1, NAMESPACE, CLUSTER_NAME, userName, messagesCount, "TLS", CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE))
            );
        });

        LOGGER.info("Check metrics exported by Kafka Exporter");
        Map<String, String> kafkaExporterMetricsData = MetricsUtils.collectKafkaExporterPodsMetrics(CLUSTER_NAME);
        assertThat("Kafka Exporter metrics should be non-empty", kafkaExporterMetricsData.size() > 0);
        for (Map.Entry<String, String> entry : kafkaExporterMetricsData.entrySet()) {
            assertThat("Value from collected metric should be non-empty", !entry.getValue().isEmpty());
            assertThat("Metrics doesn't contain specific values", entry.getValue().contains("kafka_consumergroup_current_offset"));
            assertThat("Metrics doesn't contain specific values", entry.getValue().contains("kafka_topic_partitions{topic=\"" + topic0 + "\"} 1"));
            assertThat("Metrics doesn't contain specific values", entry.getValue().contains("kafka_topic_partitions{topic=\"" + topic1 + "\"} 1"));
        }
    }

    @Test
    @Tag(NETWORKPOLICIES_SUPPORTED)
    void testNetworkPoliciesWithTlsListener() throws InterruptedException {
        Map<String, String> matchLabelsForTls = new HashMap<>();
        matchLabelsForTls.put("app", CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS);

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .withNewTls()
                            .withNewKafkaListenerAuthenticationScramSha512Auth()
                            .endKafkaListenerAuthenticationScramSha512Auth()
                            .withNetworkPolicyPeers(
                                new NetworkPolicyPeerBuilder()
                                    .withNewPodSelector()
                                        .withMatchLabels(matchLabelsForTls)
                                    .endPodSelector()
                                    .build())
                        .endTls()
                    .endListeners()
                .endKafka()
            .endSpec()
            .done();

        String topic0 = "topic-example-0";
        String topic1 = "topic-example-1";
        KafkaTopicResource.topic(CLUSTER_NAME, topic0).done();
        KafkaTopicResource.topic(CLUSTER_NAME, topic1).done();

        String userName = "user-example";
        KafkaUser kafkaUser = KafkaUserResource.scramShaUser(CLUSTER_NAME, userName).done();
        SecretUtils.waitForSecretReady(userName);

        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, kafkaUser).done();

        String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        LOGGER.info("Verifying that {} pod is able to exchange messages", kafkaClientsPodName);

        internalKafkaClient.setPodName(kafkaClientsPodName);

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(topic0, NAMESPACE, CLUSTER_NAME, userName, messagesCount, "TLS"),
            internalKafkaClient.receiveMessagesTls(topic0, NAMESPACE, CLUSTER_NAME, userName, messagesCount, "TLS", CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE))
        );

        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS + "-new", kafkaUser).done();

        String kafkaClientsNewPodName = kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(1).getMetadata().getName();

        internalKafkaClient.setPodName(kafkaClientsNewPodName);

        LOGGER.info("Verifying that {} pod is  not able to exchange messages", kafkaClientsNewPodName);
        assertThrows(AssertionError.class, () -> {
            internalKafkaClient.checkProducedAndConsumedMessages(
                internalKafkaClient.sendMessagesTls(topic1, NAMESPACE, CLUSTER_NAME, userName, messagesCount, "TLS"),
                internalKafkaClient.receiveMessagesTls(topic1, NAMESPACE, CLUSTER_NAME, userName, messagesCount, "TLS", CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE))
            );
        });
    }

    @Test
    void testTlsHostnameVerificationWithKafkaConnect() {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1).done();
        LOGGER.info("Getting IP of the bootstrap service");

        String ipOfBootstrapService = kubeClient().getService(KafkaResources.bootstrapServiceName(CLUSTER_NAME)).getSpec().getClusterIP();

        LOGGER.info("Kafka connect without config {} will not connect to {}:9093", "ssl.endpoint.identification.algorithm", ipOfBootstrapService);

        KafkaConnect kafkaConnect = KafkaConnectResource.kafkaConnectWithoutWait(KafkaConnectResource.defaultKafkaConnect(CLUSTER_NAME, CLUSTER_NAME, 1)
            .editMetadata()
                .addToLabels("type", "kafka-connect")
            .endMetadata()
            .editSpec()
                .withNewTls()
                    .addNewTrustedCertificate()
                        .withSecretName(CLUSTER_NAME + "-cluster-ca-cert")
                        .withCertificate("ca.crt")
                    .endTrustedCertificate()
                .endTls()
                .withBootstrapServers(ipOfBootstrapService + ":9093")
            .endSpec()
            .build());

        PodUtils.waitUntilPodIsPresent(CLUSTER_NAME + "-connect");

        String kafkaConnectPodName = kubeClient().listPods("type", "kafka-connect").get(0).getMetadata().getName();

        PodUtils.waitUntilPodIsInCrashLoopBackOff(kafkaConnectPodName);

        assertThat("CrashLoopBackOff", is(kubeClient().getPod(kafkaConnectPodName).getStatus().getContainerStatuses()
                .get(0).getState().getWaiting().getReason()));

        KafkaConnectResource.replaceKafkaConnectResource(CLUSTER_NAME, kc -> {
            kc.getSpec().getConfig().put("ssl.endpoint.identification.algorithm", "");
        });

        LOGGER.info("Kafka connect with config {} will connect to {}:9093", "ssl.endpoint.identification.algorithm", ipOfBootstrapService);

        TestUtils.waitFor("Waiting till kafka connect status will be 'Ready'", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> KafkaConnectResource.kafkaConnectClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus()
                        .getConditions().get(0).getType().equals("Ready"));

        KafkaConnectStatus kafkaStatus = KafkaConnectResource.kafkaConnectClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus();

        assertThat("Kafka connect status should be " + "Ready", kafkaStatus.getConditions().get(0).getType(), is("Ready"));

        KafkaConnectResource.deleteKafkaConnectWithoutWait(kafkaConnect);
    }

    @Test
    void testTlsHostnameVerificationWithMirrorMaker() {
        String sourceKafkaCluster = CLUSTER_NAME + "-source";
        String targetKafkaCluster = CLUSTER_NAME + "-target";

        KafkaResource.kafkaEphemeral(sourceKafkaCluster, 1, 1).done();
        KafkaResource.kafkaEphemeral(targetKafkaCluster, 1, 1).done();

        LOGGER.info("Getting IP of the source bootstrap service for consumer");
        String ipOfSourceBootstrapService = kubeClient().getService(KafkaResources.bootstrapServiceName(sourceKafkaCluster)).getSpec().getClusterIP();

        LOGGER.info("Getting IP of the target bootstrap service for producer");
        String ipOfTargetBootstrapService = kubeClient().getService(KafkaResources.bootstrapServiceName(targetKafkaCluster)).getSpec().getClusterIP();

        LOGGER.info("Mirror maker without config {} will not connect to consumer with address {}:9093", "ssl.endpoint.identification.algorithm", ipOfSourceBootstrapService);
        LOGGER.info("Mirror maker without config {} will not connect to producer with address {}:9093", "ssl.endpoint.identification.algorithm", ipOfTargetBootstrapService);

        KafkaMirrorMaker kafkaMirrorMaker = KafkaMirrorMakerResource.kafkaMirrorMakerWithoutWait(KafkaMirrorMakerResource.defaultKafkaMirrorMaker(CLUSTER_NAME, sourceKafkaCluster, targetKafkaCluster,
            "my-group" + rng.nextInt(Integer.MAX_VALUE), 1, true)
            .editMetadata()
                .addToLabels("type", "kafka-mirror-maker")
            .endMetadata()
            .editSpec()
                .editConsumer()
                    .withNewTls()
                        .addNewTrustedCertificate()
                            .withSecretName(KafkaResources.clusterCaCertificateSecretName(sourceKafkaCluster))
                            .withCertificate("ca.crt")
                        .endTrustedCertificate()
                    .endTls()
                    .withBootstrapServers(ipOfSourceBootstrapService + ":9093")
                .endConsumer()
                .editProducer()
                    .withNewTls()
                        .addNewTrustedCertificate()
                            .withSecretName(KafkaResources.clusterCaCertificateSecretName(targetKafkaCluster))
                            .withCertificate("ca.crt")
                        .endTrustedCertificate()
                    .endTls()
                    .withBootstrapServers(ipOfTargetBootstrapService + ":9093")
                .endProducer()
            .endSpec()
            .build());

        PodUtils.waitUntilPodIsPresent(CLUSTER_NAME + "-mirror-maker");

        String kafkaMirrorMakerPodName = kubeClient().listPods("type", "kafka-mirror-maker").get(0).getMetadata().getName();

        PodUtils.waitUntilPodIsInCrashLoopBackOff(kafkaMirrorMakerPodName);

        assertThat("CrashLoopBackOff", is(kubeClient().getPod(kafkaMirrorMakerPodName).getStatus().getContainerStatuses().get(0)
                .getState().getWaiting().getReason()));

        LOGGER.info("Mirror maker with config {} will connect to consumer with address {}:9093", "ssl.endpoint.identification.algorithm", ipOfSourceBootstrapService);
        LOGGER.info("Mirror maker with config {} will connect to producer with address {}:9093", "ssl.endpoint.identification.algorithm", ipOfTargetBootstrapService);

        LOGGER.info("Adding configuration {} to the mirror maker...", "ssl.endpoint.identification.algorithm");
        KafkaMirrorMakerResource.replaceMirrorMakerResource(CLUSTER_NAME, mm -> {
            mm.getSpec().getConsumer().getConfig().put("ssl.endpoint.identification.algorithm", ""); // disable hostname verification
            mm.getSpec().getProducer().getConfig().put("ssl.endpoint.identification.algorithm", ""); // disable hostname verification
        });

        TestUtils.waitFor("Waiting till kafka mirror maker status will be 'Ready'", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> KafkaMirrorMakerResource.kafkaMirrorMakerClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus().getConditions().get(0).getType().equals("Ready"));

        KafkaMirrorMakerStatus kafkaMirrorMakerStatus = KafkaMirrorMakerResource.kafkaMirrorMakerClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus();

        assertThat("Kafka mirror maker status should be " + "Ready", kafkaMirrorMakerStatus.getConditions().get(0).getType(), is("Ready"));

        LOGGER.info("Mirror maker connect to the kafka broker...");

        KafkaMirrorMakerResource.deleteKafkaMirrorMakerWithoutWait(kafkaMirrorMaker);
    }

    @Test
    @Tag(NODEPORT_SUPPORTED)
    void testAclRuleReadAndWrite() throws Exception {
        final String kafkaUserWrite = "kafka-user-write";
        final String kafkaUserRead = "kafka-user-read";
        final String topicName = "my-topic-name-1";
        final int numberOfMessages = 500;
        final String consumerGroupName = "consumer-group-name-1";

        KafkaResource.kafkaEphemeral(CLUSTER_NAME,  3, 1)
            .editMetadata()
                .addToLabels("type", "kafka-ephemeral")
            .endMetadata()
            .editSpec()
                .editKafka()
                .withNewKafkaAuthorizationSimple()
                .endKafkaAuthorizationSimple()
                .editListeners()
                    .withNewKafkaListenerExternalNodePort()
                        .withNewKafkaListenerAuthenticationTlsAuth()
                        .endKafkaListenerAuthenticationTlsAuth()
                    .endKafkaListenerExternalNodePort()
                .endListeners()
                .endKafka()
            .endSpec()
            .done();

        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();

        KafkaUserResource.tlsUser(CLUSTER_NAME, kafkaUserWrite)
            .editSpec()
                .withNewKafkaUserAuthorizationSimple()
                    .addNewAcl()
                        .withNewAclRuleTopicResource()
                            .withName(topicName)
                        .endAclRuleTopicResource()
                        .withOperation(AclOperation.WRITE)
                    .endAcl()
                    .addNewAcl()
                        .withNewAclRuleTopicResource()
                            .withName(topicName)
                        .endAclRuleTopicResource()
                        .withOperation(AclOperation.DESCRIBE)  // describe is for that user can find out metadata
                    .endAcl()
                .endKafkaUserAuthorizationSimple()
            .endSpec()
            .done();

        SecretUtils.waitForSecretReady(kafkaUserWrite);

        LOGGER.info("Checking kafka user:{} that is able to send messages to topic:{}", kafkaUserWrite, topicName);

        Future<Integer> producer = externalBasicKafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, kafkaUserWrite, numberOfMessages, "SSL");
        assertThat(producer.get(2, TimeUnit.MINUTES), is(numberOfMessages));

        assertThrows(ExecutionException.class, () -> {
            Future<Integer> consumer = externalBasicKafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, kafkaUserWrite,
                    numberOfMessages, "SSL");
            consumer.get(2, TimeUnit.MINUTES);
        });

        KafkaUserResource.tlsUser(CLUSTER_NAME, kafkaUserRead)
            .editSpec()
                .withNewKafkaUserAuthorizationSimple()
                    .addNewAcl()
                        .withNewAclRuleTopicResource()
                            .withName(topicName)
                        .endAclRuleTopicResource()
                        .withOperation(AclOperation.READ)
                    .endAcl()
                    .addNewAcl()
                        .withNewAclRuleGroupResource()
                            .withName(consumerGroupName)
                        .endAclRuleGroupResource()
                        .withOperation(AclOperation.READ)
                    .endAcl()
                    .addNewAcl()
                        .withNewAclRuleTopicResource()
                            .withName(topicName)
                        .endAclRuleTopicResource()
                        .withOperation(AclOperation.DESCRIBE)  // describe is for that user can find out metadata
                    .endAcl()
                .endKafkaUserAuthorizationSimple()
            .endSpec()
            .done();

        SecretUtils.waitForSecretReady(kafkaUserRead);

        Future<Integer> consumer = externalBasicKafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME,
                kafkaUserRead, numberOfMessages, "SSL", consumerGroupName);
        assertThat(consumer.get(2, TimeUnit.MINUTES), is(numberOfMessages));

        LOGGER.info("Checking kafka user:{} that is not able to send messages to topic:{}", kafkaUserRead, topicName);
        assertThrows(ExecutionException.class, () -> {
            Future<Integer> invalidProducer = externalBasicKafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, kafkaUserRead, numberOfMessages, "SSL");
            invalidProducer.get(2, TimeUnit.MINUTES);
        });
    }

    @Test
    void testAclWithSuperUser() throws Exception {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME,  3, 1)
            .editMetadata()
                .addToLabels("type", "kafka-ephemeral")
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withNewKafkaAuthorizationSimple()
                        .withSuperUsers("CN=" + USER_NAME)
                    .endKafkaAuthorizationSimple()
                    .editListeners()
                        .withNewKafkaListenerExternalNodePort()
                            .withNewKafkaListenerAuthenticationTlsAuth()
                            .endKafkaListenerAuthenticationTlsAuth()
                        .endKafkaListenerExternalNodePort()
                    .endListeners()
                .endKafka()
            .endSpec()
            .done();

        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME).done();

        KafkaUserResource.tlsUser(CLUSTER_NAME, USER_NAME)
            .editSpec()
                .withNewKafkaUserAuthorizationSimple()
                    .addNewAcl()
                        .withNewAclRuleTopicResource()
                            .withName(TOPIC_NAME)
                        .endAclRuleTopicResource()
                        .withOperation(AclOperation.WRITE)
                    .endAcl()
                    .addNewAcl()
                        .withNewAclRuleTopicResource()
                            .withName(TOPIC_NAME)
                        .endAclRuleTopicResource()
                        .withOperation(AclOperation.DESCRIBE)  // describe is for that user can find out metadata
                    .endAcl()
                .endKafkaUserAuthorizationSimple()
            .endSpec()
            .done();

        LOGGER.info("Checking kafka super user:{} that is able to send messages to topic:{}", USER_NAME, TOPIC_NAME);

        Future<Integer> producer = externalBasicKafkaClient.sendMessagesTls(TOPIC_NAME, NAMESPACE, CLUSTER_NAME, USER_NAME, MESSAGE_COUNT,
                "SSL");
        assertThat(producer.get(Constants.GLOBAL_CLIENTS_TIMEOUT, TimeUnit.MILLISECONDS), is(MESSAGE_COUNT));

        LOGGER.info("Checking kafka super user:{} that is able to read messages to topic:{} regardless that " +
                "we configured Acls with only write operation", USER_NAME, TOPIC_NAME);

        Future<Integer> consumer = externalBasicKafkaClient.receiveMessagesTls(TOPIC_NAME, NAMESPACE, CLUSTER_NAME, USER_NAME, MESSAGE_COUNT,
                "SSL");
        assertThat(consumer.get(Constants.GLOBAL_CLIENTS_TIMEOUT, TimeUnit.MILLISECONDS), is(MESSAGE_COUNT));

        String nonSuperuserName = USER_NAME + "-non-super-user";

        KafkaUserResource.tlsUser(CLUSTER_NAME, nonSuperuserName)
            .editSpec()
                .withNewKafkaUserAuthorizationSimple()
                    .addNewAcl()
                        .withNewAclRuleTopicResource()
                            .withName(TOPIC_NAME)
                        .endAclRuleTopicResource()
                        .withOperation(AclOperation.WRITE)
                    .endAcl()
                    .addNewAcl()
                        .withNewAclRuleTopicResource()
                            .withName(TOPIC_NAME)
                        .endAclRuleTopicResource()
                        .withOperation(AclOperation.DESCRIBE)  // describe is for that user can find out metadata
                    .endAcl()
                .endKafkaUserAuthorizationSimple()
            .endSpec()
            .done();

        LOGGER.info("Checking kafka super user:{} that is able to send messages to topic:{}", nonSuperuserName, TOPIC_NAME);

        externalBasicKafkaClient.sendMessagesTls(TOPIC_NAME, NAMESPACE, CLUSTER_NAME, nonSuperuserName, MESSAGE_COUNT,
                "SSL");
        assertThat(producer.get(Constants.GLOBAL_CLIENTS_TIMEOUT, TimeUnit.MILLISECONDS), is(MESSAGE_COUNT));
        
        LOGGER.info("Checking kafka super user:{} that is not able to read messages to topic:{} because of defined" +
                " ACLs on only write operation", nonSuperuserName, TOPIC_NAME);

        assertThrows(ExecutionException.class, () -> {
            Future<Integer> invalidConsumer = externalBasicKafkaClient.receiveMessagesTls(TOPIC_NAME, NAMESPACE, CLUSTER_NAME, nonSuperuserName, MESSAGE_COUNT,
                    "SSL");
            invalidConsumer.get(Duration.ofSeconds(10).toMillis(), TimeUnit.MILLISECONDS);
        });
    }

    @Test
    void testCaRenewalBreakInMiddle() {
        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3, 3)
            .editSpec()
                .withNewClusterCa()
                    .withRenewalDays(1)
                    .withValidityDays(3)
                .endClusterCa()
            .endSpec().done();

        String userName = "alice";
        KafkaUser user = KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();
        String topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);

        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, user).done();

        String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        internalKafkaClient.setPodName(defaultKafkaClientsPodName);

        int sent = internalKafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, messagesCount, "TLS");
        assertThat(sent, is(messagesCount));
        int received = internalKafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, messagesCount, "TLS", CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE));
        assertThat(received, is(sent));

        Map<String, String> zkPods = StatefulSetUtils.ssSnapshot(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME));
        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));
        Map<String, String> eoPods = DeploymentUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME));
        String clusterCaCert = createSecret("cluster-ca.crt", clusterCaCertificateSecretName(CLUSTER_NAME), "ca.crt");

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec()
                .getZookeeper()
                .setResources(new ResourceRequirementsBuilder()
                    .addToRequests("cpu", new Quantity("100000m"))
                    .build());
            k.getSpec().setClusterCa(new CertificateAuthorityBuilder()
                .withRenewalDays(4)
                .withValidityDays(7)
                .build());
        });

        StUtils.waitForReconciliation(testClass, testName, NAMESPACE);

        received = internalKafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, messagesCount, "TLS", CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE));
        assertThat(received, is(sent));

        StUtils.waitForReconciliation(testClass, testName, NAMESPACE);
        List<String> podStatuses = kubeClient().listPods().stream()
            .filter(p -> p.getMetadata().getName().startsWith(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME))
                    && p.getMetadata().getLabels().containsKey("strimzi.io/kind")
                    && p.getMetadata().getLabels().containsValue("Kafka"))
            .map(p -> p.getStatus().getPhase()).sorted().collect(Collectors.toList());
        assertThat(podStatuses, hasItem("Pending"));

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec()
                .getZookeeper()
                .setResources(new ResourceRequirementsBuilder()
                    .addToRequests("cpu", new Quantity("200m"))
                    .build());
        });

        // Wait until the certificates have been replaced
        waitForCertToChange(clusterCaCert, KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME));
        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), 3, zkPods);
        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);
        DeploymentUtils.waitTillDepHasRolled(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), 1, eoPods);

        LOGGER.info("Checking produced and consumed messages to pod:{}", internalKafkaClient.getPodName());
        received = internalKafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, messagesCount, "TLS", CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE));
        assertThat(received, is(sent));

        // Try to send and receive messages with new certificates
        topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);
        sent = internalKafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, messagesCount, "TLS");
        assertThat(sent, is(messagesCount));
        received = internalKafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, userName, messagesCount, "TLS", CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE));
        assertThat(received, is(sent));
    }

    @BeforeAll
    void setup() {
        ResourceManager.setClassResources();
        prepareEnvForOperator(NAMESPACE);

        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        KubernetesResource.clusterOperator(NAMESPACE, Constants.CO_OPERATION_TIMEOUT_MEDIUM).done();
    }
}
