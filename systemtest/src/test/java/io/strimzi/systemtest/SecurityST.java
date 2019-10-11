/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyPeerBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(REGRESSION)
class SecurityST extends MessagingBaseST {

    public static final String NAMESPACE = "security-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(SecurityST.class);
    private static final String OPENSSL_RETURN_CODE = "Verify return code: 0 (ok)";
    private static final String TLS_PROTOCOL = "Protocol  : TLSv1";
    private static final String SSL_TIMEOUT = "Timeout   : 300 (sec)";
    public static final String STRIMZI_IO_FORCE_RENEW = "strimzi.io/force-renew";
    public static final String STRIMZI_IO_FORCE_REPLACE = "strimzi.io/force-replace";

    @Test
    void testCertificates() {
        LOGGER.info("Running testCertificates {}", CLUSTER_NAME);
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 2)
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
                if (e.result != null && e.result.exitStatus() == 104) {
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
            boolean eoShouldRoll) throws Exception {
        createClusterWithExternalRoute();
        String userName = "alice";
        testMethodResources().tlsUser(CLUSTER_NAME, userName).done();
        waitFor("", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_GET_SECRETS, () -> kubeClient().getSecret("alice") != null,
            () -> LOGGER.error("Couldn't find user secret {}", kubeClient().listSecrets()));

        waitForClusterAvailabilityTls(userName, NAMESPACE, CLUSTER_NAME);

        // Get all pods, and their resource versions
        Map<String, String> zkPods = StUtils.ssSnapshot(zookeeperStatefulSetName(CLUSTER_NAME));
        Map<String, String> kafkaPods = StUtils.ssSnapshot(kafkaStatefulSetName(CLUSTER_NAME));
        Map<String, String> eoPod = StUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME));

        LOGGER.info("Triggering CA cert renewal by adding the annotation");
        Map<String, String> initialCaCerts = new HashMap<>();
        for (String secretName : secretsToAnnotate) {
            Secret secret = kubeClient().getSecret(secretName);
            String value = secret.getData().get("ca.crt");
            assertNotNull("ca.crt in " + secretName + " should not be null", value);
            initialCaCerts.put(secretName, value);
            Secret annotated = new SecretBuilder(secret)
                    .editMetadata()
                    .addToAnnotations(STRIMZI_IO_FORCE_RENEW, "true")
                    .endMetadata()
                    .build();
            LOGGER.info("Patching secret {} with {}", secretName, STRIMZI_IO_FORCE_RENEW);
            kubeClient().patchSecret(secretName, annotated);
        }

        if (zkShouldRoll) {
            LOGGER.info("Wait for zk to rolling restart ...");
            StUtils.waitTillSsHasRolled(zookeeperStatefulSetName(CLUSTER_NAME), 3, zkPods);
        }
        if (kafkaShouldRoll) {
            LOGGER.info("Wait for kafka to rolling restart ...");
            StUtils.waitTillSsHasRolled(kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);
        }
        if (eoShouldRoll) {
            LOGGER.info("Wait for EO to rolling restart ...");
            eoPod = StUtils.waitTillDepHasRolled(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), 1, eoPod);
        }

        LOGGER.info("Checking the certificates have been replaced");
        for (String secretName : secretsToAnnotate) {
            Secret secret = kubeClient().getSecret(secretName);
            assertNotNull(secret, "Secret " + secretName + " should exist");
            assertNotNull(secret.getData(), "CA cert in " + secretName + " should have non-null 'data'");
            String value = secret.getData().get("ca.crt");
            assertNotEquals("CA cert in " + secretName + " should have changed",
                    initialCaCerts.get(secretName), value);
        }

        waitForClusterAvailabilityTls(userName, NAMESPACE, CLUSTER_NAME);

        // Check a new client (signed by new client key) can consume
        String bobUserName = "bob";
        testMethodResources().tlsUser(CLUSTER_NAME, bobUserName).done();
        StUtils.waitForSecretReady(bobUserName);

        waitForClusterAvailabilityTls(bobUserName, NAMESPACE, CLUSTER_NAME);

        if (!zkShouldRoll) {
            assertEquals(zkPods, StUtils.ssSnapshot(zookeeperStatefulSetName(CLUSTER_NAME)),
                    "ZK pods should not roll, but did.");

        }
        if (!kafkaShouldRoll) {
            assertEquals(kafkaPods, StUtils.ssSnapshot(kafkaStatefulSetName(CLUSTER_NAME)),
                    "Kafka pods should not roll, but did.");

        }
        if (!eoShouldRoll) {
            assertEquals(eoPod, StUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME)),
                    "EO pod should not roll, but did.");
        }
    }

    @Test
    @OpenShiftOnly
    @Tag(ACCEPTANCE)
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
    @OpenShiftOnly
    @Tag(ACCEPTANCE)
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
    @OpenShiftOnly
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
        createClusterWithExternalRoute();
        String aliceUserName = "alice";
        testMethodResources().tlsUser(CLUSTER_NAME, aliceUserName).done();
        waitFor("Alic's secret to exist", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_GET_SECRETS,
            () -> kubeClient().getSecret(aliceUserName) != null,
            () -> LOGGER.error("Couldn't find user secret {}", kubeClient().listSecrets()));

        waitForClusterAvailabilityTls(aliceUserName, NAMESPACE, CLUSTER_NAME);

        // Get all pods, and their resource versions
        Map<String, String> zkPods = StUtils.ssSnapshot(zookeeperStatefulSetName(CLUSTER_NAME));
        Map<String, String> kafkaPods = StUtils.ssSnapshot(kafkaStatefulSetName(CLUSTER_NAME));
        Map<String, String> eoPod = StUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME));

        LOGGER.info("Triggering CA cert renewal by adding the annotation");
        Map<String, String> initialCaKeys = new HashMap<>();
        for (String secretName : secrets) {
            Secret secret = kubeClient().getSecret(secretName);
            String value = secret.getData().get("ca.key");
            assertNotNull("ca.key in " + secretName + " should not be null", value);
            initialCaKeys.put(secretName, value);
            Secret annotated = new SecretBuilder(secret)
                    .editMetadata()
                    .addToAnnotations(STRIMZI_IO_FORCE_REPLACE, "true")
                    .endMetadata()
                    .build();
            LOGGER.info("Patching secret {} with {}", secretName, STRIMZI_IO_FORCE_REPLACE);
            kubeClient().patchSecret(secretName, annotated);
        }

        if (zkShouldRoll) {
            LOGGER.info("Wait for zk to rolling restart (1)...");
            zkPods = StUtils.waitTillSsHasRolled(zookeeperStatefulSetName(CLUSTER_NAME), 3, zkPods);
        }
        if (kafkaShouldRoll) {
            LOGGER.info("Wait for kafka to rolling restart (1)...");
            kafkaPods = StUtils.waitTillSsHasRolled(kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);
        }
        if (eoShouldRoll) {
            LOGGER.info("Wait for EO to rolling restart (1)...");
            eoPod = StUtils.waitTillDepHasRolled(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), 1, eoPod);
        }

        if (zkShouldRoll) {
            LOGGER.info("Wait for zk to rolling restart (2)...");
            zkPods = StUtils.waitTillSsHasRolled(zookeeperStatefulSetName(CLUSTER_NAME), 3, zkPods);
        }
        if (kafkaShouldRoll) {
            LOGGER.info("Wait for kafka to rolling restart (2)...");
            kafkaPods = StUtils.waitTillSsHasRolled(kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);
        }
        if (eoShouldRoll) {
            LOGGER.info("Wait for EO to rolling restart (2)...");
            eoPod = StUtils.waitTillDepHasRolled(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), 1, eoPod);
        }

        LOGGER.info("Checking the certificates have been replaced");
        for (String secretName : secrets) {
            Secret secret = kubeClient().getSecret(secretName);
            assertNotNull(secret, "Secret " + secretName + " should exist");
            assertNotNull(secret.getData(), "CA key in " + secretName + " should have non-null 'data'");
            String value = secret.getData().get("ca.key");
            assertNotNull("CA key in " + secretName + " should exist", value);
            assertNotEquals("CA key in " + secretName + " should have changed",
                    initialCaKeys.get(secretName), value);
        }

        waitForClusterAvailabilityTls(aliceUserName, NAMESPACE, CLUSTER_NAME);

        // Finally check a new client (signed by new client key) can consume
        String bobUserName = "bob";
        testMethodResources().tlsUser(CLUSTER_NAME, bobUserName).done();
        waitFor("Bob's secret to exist", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_GET_SECRETS,
            () -> kubeClient().getSecret(bobUserName) != null,
            () -> LOGGER.error("Couldn't find user secret {}", kubeClient().listSecrets()));

        waitForClusterAvailabilityTls(bobUserName, NAMESPACE, CLUSTER_NAME);

        if (!zkShouldRoll) {
            assertEquals(zkPods, StUtils.ssSnapshot(zookeeperStatefulSetName(CLUSTER_NAME)),
                    "ZK pods should not roll, but did.");

        }
        if (!kafkaShouldRoll) {
            assertEquals(kafkaPods, StUtils.ssSnapshot(kafkaStatefulSetName(CLUSTER_NAME)),
                    "Kafka pods should not roll, but did.");

        }
        if (!eoShouldRoll) {
            assertEquals(eoPod, StUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME)),
                    "EO pod should not roll, but did.");
        }
    }

    @Test
    @OpenShiftOnly
    void testAutoReplaceClusterCaKeysTriggeredByAnno() throws Exception {
        autoReplaceSomeKeysTriggeredByAnno(asList(clusterCaKeySecretName(CLUSTER_NAME)),
                true,
                true,
                true);
    }

    @Test
    @OpenShiftOnly
    void testAutoReplaceClientsCaKeysTriggeredByAnno() throws Exception {
        autoReplaceSomeKeysTriggeredByAnno(asList(clientsCaKeySecretName(CLUSTER_NAME)),
                false,
                true,
                true);
    }

    @Test
    @OpenShiftOnly
    void testAutoReplaceAllCaKeysTriggeredByAnno() throws Exception {
        autoReplaceSomeKeysTriggeredByAnno(asList(clusterCaKeySecretName(CLUSTER_NAME),
                clientsCaKeySecretName(CLUSTER_NAME)),
                true,
                true,
                true);
    }

    private void createClusterWithExternalRoute() {
        LOGGER.info("Creating a cluster");
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3)
                .editSpec()
                    .editKafka()
                        .editListeners()
                            .withNewKafkaListenerExternalRoute()
                            .endKafkaListenerExternalRoute()
                        .endListeners()
                        .withConfig(singletonMap("default.replication.factor", 3))
                        .withNewPersistentClaimStorage()
                            .withSize("2Gi")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endKafka()
                    .editZookeeper()
                        .withReplicas(3)
                        .withNewPersistentClaimStorage()
                            .withSize("2Gi")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .done();
    }

    @Test
    @OpenShiftOnly
    void testAutoRenewCaCertsTriggerByExpiredCertificate() throws Exception {
        // 1. Create the Secrets already, and a certificate that's already expired
        String clusterCaKey = createSecret("cluster-ca.key", clusterCaKeySecretName(CLUSTER_NAME), "ca.key");
        String clusterCaCert = createSecret("cluster-ca.crt", clusterCaCertificateSecretName(CLUSTER_NAME), "ca.crt");
        String clientsCaKey = createSecret("clients-ca.key", clientsCaKeySecretName(CLUSTER_NAME), "ca.key");
        String clientsCaCert = createSecret("clients-ca.crt", clientsCaCertificateSecretName(CLUSTER_NAME), "ca.crt");

        // 2. Now create a cluster
        createClusterWithExternalRoute();
        String userName = "alice";
        testMethodResources().tlsUser(CLUSTER_NAME, userName).done();
        // Check if user exists
        StUtils.waitForSecretReady(userName);

        waitForClusterAvailabilityTls(userName, NAMESPACE, CLUSTER_NAME);

        // Wait until the certificates have been replaced
        waitForCertToChange(clusterCaCert, clusterCaCertificateSecretName(CLUSTER_NAME));

        // Wait until the pods are all up and ready
        waitForClusterStability();

        waitForClusterAvailabilityTls(userName, NAMESPACE, CLUSTER_NAME);
    }

    @SuppressWarnings("unchecked")
    private void waitForClusterStability() {
        LOGGER.info("Waiting for cluster stability");
        Map<String, String>[] zkPods = new Map[1];
        Map<String, String>[] kafkaPods = new Map[1];
        Map<String, String>[] eoPods = new Map[1];
        AtomicInteger count = new AtomicInteger();
        zkPods[0] = StUtils.ssSnapshot(zookeeperStatefulSetName(CLUSTER_NAME));
        kafkaPods[0] = StUtils.ssSnapshot(kafkaStatefulSetName(CLUSTER_NAME));
        eoPods[0] = StUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME));
        TestUtils.waitFor("Cluster stable and ready", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_CLUSTER_STABLE, () -> {
            Map<String, String> zkSnapshot = StUtils.ssSnapshot(zookeeperStatefulSetName(CLUSTER_NAME));
            Map<String, String> kafkaSnaptop = StUtils.ssSnapshot(kafkaStatefulSetName(CLUSTER_NAME));
            Map<String, String> eoSnapshot = StUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME));
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
                        "strimzi.io/cluster", CLUSTER_NAME,
                        "strimzi.io/kind", "Kafka"))
                .endMetadata()
                .withData(singletonMap(keyName, Base64.getEncoder().encodeToString(certAsString.getBytes(StandardCharsets.US_ASCII))))
            .build();
        kubeClient().createSecret(secret);
        return certAsString;
    }

    @Test
    @Tag(NODEPORT_SUPPORTED)
    void testCertRenewalInMaintenanceWindow() throws Exception {
        String secretName = CLUSTER_NAME + "-cluster-ca-cert";
        LocalDateTime maintenanceWindowStart = LocalDateTime.now().withSecond(0);
        long maintenanceWindowDuration = 4;
        maintenanceWindowStart = maintenanceWindowStart.plusMinutes(maintenanceWindowDuration);
        long windowStartMin = maintenanceWindowStart.getMinute();
        long windowStopMin = windowStartMin + maintenanceWindowDuration > 59
                ? windowStartMin + maintenanceWindowDuration - 60 : windowStartMin + maintenanceWindowDuration;

        String maintenanceWindowCron = "* " + windowStartMin + "-" + windowStopMin + " * * * ? *";
        LOGGER.info("Maintenance window is: {}", maintenanceWindowCron);
        testMethodResources.kafkaEphemeral(CLUSTER_NAME, 3, 1)
                .editSpec()
                .editKafka()
                .editListeners()
                .withNewKafkaListenerExternalNodePort()
                .withTls(false)
                .endKafkaListenerExternalNodePort()
                .endListeners()
                .endKafka()
                .addNewMaintenanceTimeWindow(maintenanceWindowCron)
                .endSpec().done();

        Map<String, String> kafkaPods = StUtils.ssSnapshot(kafkaStatefulSetName(CLUSTER_NAME));

        LOGGER.info("Annotate secret {} with secret force-renew annotation", secretName);
        Secret secret = new SecretBuilder(kubeClient().getSecret(secretName))
                .editMetadata()
                .addToAnnotations("strimzi.io/force-renew", "true")
                .endMetadata().build();
        kubeClient().patchSecret(secretName, secret);

        LOGGER.info("Wait until maintenance windows starts");
        LocalDateTime finalMaintenanceWindowStart = maintenanceWindowStart;
        TestUtils.waitFor("Wait until maintenance window start",
            Constants.GLOBAL_POLL_INTERVAL, Duration.ofMinutes(maintenanceWindowDuration).toMillis() - 10000,
            () -> LocalDateTime.now().isAfter(finalMaintenanceWindowStart));

        LOGGER.info("Maintenance window starts");

        assertThat("Rolling update was performed out of maintenance window!", kafkaPods, is(StUtils.ssSnapshot(kafkaStatefulSetName(CLUSTER_NAME))));

        LOGGER.info("Wait until rolling update is triggered during maintenance window");
        StUtils.waitTillSsHasRolled(kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);

        assertThat("Rolling update wasn't performed in correct time", LocalDateTime.now().isAfter(maintenanceWindowStart));
        waitForClusterAvailability(NAMESPACE);
    }

    @Test
    @Tag(NODEPORT_SUPPORTED)
    void testCertRegeneratedAfterInternalCAisDeleted() throws Exception {

        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3, 1)
                .editSpec()
                    .editKafka()
                        .editListeners()
                            .withNewKafkaListenerExternalNodePort()
                            .endKafkaListenerExternalNodePort()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .done();

        Map<String, String> kafkaPods = StUtils.ssSnapshot(kafkaStatefulSetName(CLUSTER_NAME));

        String userName = "user-example";
        testMethodResources().tlsUser(CLUSTER_NAME, userName).done();
        StUtils.waitForSecretReady(userName);

        List<Secret> secrets = kubeClient().listSecrets().stream()
                .filter(secret -> secret.getMetadata().getName().endsWith("ca-cert"))
                .collect(Collectors.toList());

        for (Secret s : secrets) {
            LOGGER.info("Verifying that secret {} with name {} is present", s, s.getMetadata().getName());
            assertNotNull(s.getData());
        }

        for (Secret s : secrets) {
            LOGGER.info("Deleting secret {}", s.getMetadata().getName());
            kubeClient().deleteSecret(s.getMetadata().getName());
        }

        StUtils.waitForReconciliation(testClass, testName, NAMESPACE);
        StUtils.waitTillSsHasRolled(kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);

        for (Secret s : secrets) {
            StUtils.waitForSecretReady(s.getMetadata().getName());
        }

        List<Secret> regeneratedSecrets = kubeClient().listSecrets().stream()
                .filter(secret -> secret.getMetadata().getName().endsWith("ca-cert"))
                .collect(Collectors.toList());

        for (int i = 0; i < secrets.size(); i++) {
            assertThat("Certificates has different cert UIDs", !secrets.get(i).getData().get("ca.crt").equals(regeneratedSecrets.get(i).getData().get("ca.crt")));
        }

        waitForClusterAvailabilityTls(userName, NAMESPACE, CLUSTER_NAME);
    }

    @Test
    @Tag(NETWORKPOLICIES_SUPPORTED)
    void testNetworkPoliciesWithPlainListener() throws Exception {
        Map<String, String> matchLabelForPlain = new HashMap<>();
        matchLabelForPlain.put("app", CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS);

        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 1)
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
                    .endSpec()
                .done();

        String topic0 = "topic-example-0";
        String topic1 = "topic-example-1";

        String userName = "user-example";
        KafkaUser kafkaUser = testMethodResources().scramShaUser(CLUSTER_NAME, userName).done();
        StUtils.waitForSecretReady(userName);

        testMethodResources().topic(CLUSTER_NAME, topic0).done();
        testMethodResources().topic(CLUSTER_NAME, topic1).done();

        testMethodResources().deployKafkaClients(false, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, kafkaUser).done();

        String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        LOGGER.info("Verifying that {} pod is able to exchange messages", kafkaClientsPodName);
        availabilityTest(50, CLUSTER_NAME, false, topic0, kafkaUser, kafkaClientsPodName);

        testMethodResources().deployKafkaClients(false, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS + "-new", kafkaUser).done();

        String kafkaClientsNewPodName = kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(1).getMetadata().getName();

        LOGGER.info("Verifying that {} pod is not able to exchange messages", kafkaClientsNewPodName);
        assertThrows(AssertionError.class, () ->  availabilityTest(50, CLUSTER_NAME, false, topic1, kafkaUser, kafkaClientsNewPodName));
    }

    @Test
    @Tag(NETWORKPOLICIES_SUPPORTED)
    void testNetworkPoliciesWithTlsListener() throws Exception {
        Map<String, String> matchLabelsForTls = new HashMap<>();
        matchLabelsForTls.put("app", CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS);

        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 1)
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
        testMethodResources().topic(CLUSTER_NAME, topic0).done();
        testMethodResources().topic(CLUSTER_NAME, topic1).done();

        String userName = "user-example";
        KafkaUser kafkaUser = testMethodResources().scramShaUser(CLUSTER_NAME, userName).done();
        StUtils.waitForSecretReady(userName);

        testMethodResources().deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, kafkaUser).done();

        String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        LOGGER.info("Verifying that {} pod is able to exchange messages", kafkaClientsPodName);
        availabilityTest(50, CLUSTER_NAME, true, topic0, kafkaUser, kafkaClientsPodName);

        testMethodResources().deployKafkaClients(true, CLUSTER_NAME + "-kafka-clients-new", kafkaUser).done();

        String kafkaClientsNewPodName = kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(1).getMetadata().getName();

        LOGGER.info("Verifying that {} pod is  not able to exchange messages", kafkaClientsNewPodName);
        assertThrows(AssertionError.class, () -> availabilityTest(50, CLUSTER_NAME, true, topic1, kafkaUser, kafkaClientsNewPodName));
    }


    @BeforeEach
    void createTestResources() {
        createTestMethodResources();
    }

    @BeforeAll
    void setupEnvironment() {
        LOGGER.info("Creating resources before the test class");
        prepareEnvForOperator(NAMESPACE);

        createTestClassResources();
        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        testClassResources().clusterOperator(NAMESPACE).done();
    }

    @Override
    protected void tearDownEnvironmentAfterEach() throws Exception {
        deleteTestMethodResources();
        waitForDeletion(Constants.TIMEOUT_TEARDOWN);
    }

}
