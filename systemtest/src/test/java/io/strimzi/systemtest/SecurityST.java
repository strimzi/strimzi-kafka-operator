/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.annotations.OpenShiftOnly;
import io.strimzi.test.extensions.StrimziExtension;
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static io.strimzi.api.kafka.model.KafkaResources.clientsCaCertificateSecretName;
import static io.strimzi.api.kafka.model.KafkaResources.clientsCaKeySecretName;
import static io.strimzi.api.kafka.model.KafkaResources.clusterCaCertificateSecretName;
import static io.strimzi.api.kafka.model.KafkaResources.clusterCaKeySecretName;
import static io.strimzi.api.kafka.model.KafkaResources.kafkaStatefulSetName;
import static io.strimzi.api.kafka.model.KafkaResources.zookeeperStatefulSetName;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.TestUtils.map;
import static io.strimzi.test.TestUtils.waitFor;
import static io.strimzi.test.extensions.StrimziExtension.FLAKY;
import static io.strimzi.test.extensions.StrimziExtension.REGRESSION;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag(REGRESSION)
class SecurityST extends AbstractST {

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
                KUBE_CMD_CLIENT.execInPodContainer(kafkaPodName(CLUSTER_NAME, 0), "kafka",
                        "/bin/bash", "-c", commandForKafkaBootstrap).out();
        checkKafkaCertificates(outputForKafkaBootstrap);

        String commandForZookeeperClient = "echo -n | openssl s_client -connect my-cluster-zookeeper-client:2181 -showcerts" +
                " -CAfile /opt/kafka/cluster-ca-certs/ca.crt" +
                " -verify_hostname my-cluster-zookeeper-client" +
                " -cert /opt/kafka/broker-certs/my-cluster-kafka-0.crt" +
                " -key /opt/kafka/broker-certs/my-cluster-kafka-0.key";
        String outputForZookeeperClient =
                KUBE_CMD_CLIENT.execInPodContainer(kafkaPodName(CLUSTER_NAME, 0), "kafka",
                        "/bin/bash", "-c", commandForZookeeperClient).out();
        checkZookeeperCertificates(outputForZookeeperClient);

        IntStream.rangeClosed(0, 1).forEach(podId -> {
            String commandForKafkaPort9091 = "echo -n | " + generateOpenSSLCommandWithCerts(kafkaPodName(CLUSTER_NAME, podId), "my-cluster-kafka-brokers", "9091");
            String commandForKafkaPort9093 = "echo -n | " + generateOpenSSLCommandWithCAfile(kafkaPodName(CLUSTER_NAME, podId), "my-cluster-kafka-brokers", "9093");

            String outputForKafkaPort9091 =
                    KUBE_CMD_CLIENT.execInPodContainer(kafkaPodName(CLUSTER_NAME, podId), "kafka", "/bin/bash", "-c", commandForKafkaPort9091).out();
            String outputForKafkaPort9093 =
                    KUBE_CMD_CLIENT.execInPodContainer(kafkaPodName(CLUSTER_NAME, podId), "kafka", "/bin/bash", "-c", commandForKafkaPort9093).out();

            checkKafkaCertificates(outputForKafkaPort9091, outputForKafkaPort9093);

            String commandForZookeeperPort2181 = "echo -n | " + generateOpenSSLCommandWithCerts(zookeeperPodName(CLUSTER_NAME, podId), "my-cluster-zookeeper-nodes", "2181");
            String commandForZookeeperPort2888 = "echo -n | " + generateOpenSSLCommandWithCerts(zookeeperPodName(CLUSTER_NAME, podId), "my-cluster-zookeeper-nodes", "2888");
            String commandForZookeeperPort3888 = "echo -n | " + generateOpenSSLCommandWithCerts(zookeeperPodName(CLUSTER_NAME, podId), "my-cluster-zookeeper-nodes", "3888");

            String outputForZookeeperPort2181 =
                    KUBE_CMD_CLIENT.execInPodContainer(kafkaPodName(CLUSTER_NAME, podId), "kafka", "/bin/bash", "-c",
                            commandForZookeeperPort2181).out();

            String outputForZookeeperPort3888 =
                    KUBE_CMD_CLIENT.execInPodContainer(kafkaPodName(CLUSTER_NAME, podId), "kafka", "/bin/bash", "-c",
                            commandForZookeeperPort3888).out();
            checkZookeeperCertificates(outputForZookeeperPort2181, outputForZookeeperPort3888);

            try {
                String outputForZookeeperPort2888 =
                        KUBE_CMD_CLIENT.execInPodContainer(kafkaPodName(CLUSTER_NAME, podId), "kafka", "/bin/bash", "-c",
                                commandForZookeeperPort2888).out();
                checkZookeeperCertificates(outputForZookeeperPort2888);
            } catch (KubeClusterException e) {
                if (e.result != null && e.result.exitStatus() == 104) {
                    LOGGER.info("The connection for {} was forcibly closed because of new zookeeper leader", zookeeperPodName(CLUSTER_NAME, podId));
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

    @Test
    @OpenShiftOnly
    @Tag(REGRESSION)
    void testAutoRenewCaCertsTriggeredByAnno() throws Exception {
        createClusterWithExternalRoute();
        String userName = "alice";
        testMethodResources().tlsUser(CLUSTER_NAME, userName).done();
        waitFor("", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_GET_SECRETS, () -> KUBE_CLIENT.getSecret("alice") != null,
            () -> LOGGER.error("Couldn't find user secret {}", KUBE_CLIENT.listSecrets()));

        waitForClusterAvailabilityTls(userName, NAMESPACE);

        // Get all pods, and their resource versions
        Map<String, String> zkPods = StUtils.ssSnapshot(NAMESPACE, zookeeperStatefulSetName(CLUSTER_NAME));
        Map<String, String> kafkaPods = StUtils.ssSnapshot(NAMESPACE, kafkaStatefulSetName(CLUSTER_NAME));

        LOGGER.info("Triggering CA cert renewal by adding the annotation");
        Map<String, String> initialCaCerts = new HashMap<>();
        List<String> secrets = asList(clusterCaCertificateSecretName(CLUSTER_NAME),
                clientsCaCertificateSecretName(CLUSTER_NAME));
        for (String secretName : secrets) {
            Secret secret = KUBE_CLIENT.getSecret(secretName);
            String value = secret.getData().get("ca.crt");
            assertNotNull("ca.crt in " + secretName + " should not be null", value);
            initialCaCerts.put(secretName, value);
            Secret annotated = new SecretBuilder(secret)
                    .editMetadata()
                        .addToAnnotations(STRIMZI_IO_FORCE_RENEW, "true")
                    .endMetadata()
                .build();
            LOGGER.info("Patching secret {} with {}", secretName, STRIMZI_IO_FORCE_RENEW);
            KUBE_CLIENT.patchSecret(secretName, annotated);
        }

        Map<String, String> eoPod = StUtils.depSnapshot(NAMESPACE, KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME));

        LOGGER.info("Wait for zk to rolling restart ...");
        StUtils.waitTillSsHasRolled(NAMESPACE, zookeeperStatefulSetName(CLUSTER_NAME), zkPods);
        LOGGER.info("Wait for kafka to rolling restart ...");
        StUtils.waitTillSsHasRolled(NAMESPACE, kafkaStatefulSetName(CLUSTER_NAME), kafkaPods);
        LOGGER.info("Wait for EO to rolling restart ...");
        eoPod = StUtils.waitTillDepHasRolled(NAMESPACE, KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), eoPod);

        LOGGER.info("Checking the certificates have been replaced");
        for (String secretName : secrets) {
            Secret secret = KUBE_CLIENT.getSecret(secretName);
            assertNotNull(secret, "Secret " + secretName + " should exist");
            assertNotNull(secret.getData(), "CA cert in " + secretName + " should have non-null 'data'");
            String value = secret.getData().get("ca.crt");
            assertNotEquals("CA cert in " + secretName + " should have changed",
                    initialCaCerts.get(secretName), value);
        }

        waitForClusterAvailabilityTls(userName, NAMESPACE);

        // Finally check a new client (signed by new client key) can consume
        String bobUserName = "bob";
        testMethodResources().tlsUser(CLUSTER_NAME, bobUserName).done();
        waitFor("", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_GET_SECRETS, () -> {
            return KUBE_CLIENT.getSecret(bobUserName) != null;
        },
            () -> {
                LOGGER.error("Couldn't find user secret {}", KUBE_CLIENT.listSecrets());
            });

        waitForClusterAvailabilityTls(bobUserName, NAMESPACE);
    }

    @Test
    @OpenShiftOnly
    @Tag(REGRESSION)
    void testAutoReplaceCaKeysTriggeredByAnno() throws Exception {
        createClusterWithExternalRoute();
        String aliceUserName = "alice";
        resources().tlsUser(CLUSTER_NAME, aliceUserName).done();
        waitFor("Alic's secret to exist", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_GET_SECRETS,
            () -> KUBE_CLIENT.getSecret(aliceUserName) != null,
            () -> LOGGER.error("Couldn't find user secret {}", KUBE_CLIENT.listSecrets()));

        waitForClusterAvailabilityTls(aliceUserName, NAMESPACE);

        // Get all pods, and their resource versions
        Map<String, String> zkPods = StUtils.ssSnapshot(NAMESPACE, zookeeperStatefulSetName(CLUSTER_NAME));
        Map<String, String> kafkaPods = StUtils.ssSnapshot(NAMESPACE, kafkaStatefulSetName(CLUSTER_NAME));
        Map<String, String> eoPod = StUtils.depSnapshot(NAMESPACE, KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME));

        LOGGER.info("Triggering CA cert renewal by adding the annotation");
        Map<String, String> initialCaKeys = new HashMap<>();
        List<String> secrets = asList(clusterCaKeySecretName(CLUSTER_NAME),
                clientsCaKeySecretName(CLUSTER_NAME));
        for (String secretName : secrets) {
            Secret secret = KUBE_CLIENT.getSecret(secretName);
            String value = secret.getData().get("ca.key");
            assertNotNull("ca.key in " + secretName + " should not be null", value);
            initialCaKeys.put(secretName, value);
            Secret annotated = new SecretBuilder(secret)
                    .editMetadata()
                    .addToAnnotations(STRIMZI_IO_FORCE_REPLACE, "true")
                    .endMetadata()
                    .build();
            LOGGER.info("Patching secret {} with {}", secretName, STRIMZI_IO_FORCE_REPLACE);
            KUBE_CLIENT.patchSecret(secretName, annotated);
        }

        LOGGER.info("Wait for zk to rolling restart (1)...");
        zkPods = StUtils.waitTillSsHasRolled(NAMESPACE, zookeeperStatefulSetName(CLUSTER_NAME), zkPods);
        LOGGER.info("Wait for kafka to rolling restart (1)...");
        kafkaPods = StUtils.waitTillSsHasRolled(NAMESPACE, kafkaStatefulSetName(CLUSTER_NAME), kafkaPods);
        LOGGER.info("Wait for EO to rolling restart (1)...");
        eoPod = StUtils.waitTillDepHasRolled(NAMESPACE, KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), eoPod);

        LOGGER.info("Wait for zk to rolling restart (2)...");
        zkPods = StUtils.waitTillSsHasRolled(NAMESPACE, zookeeperStatefulSetName(CLUSTER_NAME), zkPods);
        LOGGER.info("Wait for kafka to rolling restart (2)...");
        kafkaPods = StUtils.waitTillSsHasRolled(NAMESPACE, kafkaStatefulSetName(CLUSTER_NAME), kafkaPods);
        LOGGER.info("Wait for EO to rolling restart (2)...");
        eoPod = StUtils.waitTillDepHasRolled(NAMESPACE, KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), eoPod);

        LOGGER.info("Checking the certificates have been replaced");
        for (String secretName : secrets) {
            Secret secret = KUBE_CLIENT.getSecret(secretName);
            assertNotNull(secret, "Secret " + secretName + " should exist");
            assertNotNull(secret.getData(), "CA key in " + secretName + " should have non-null 'data'");
            String value = secret.getData().get("ca.key");
            assertNotNull("CA key in " + secretName + " should exist", value);
            assertNotEquals("CA key in " + secretName + " should have changed",
                    initialCaKeys.get(secretName), value);
        }

        waitForClusterAvailabilityTls(aliceUserName, NAMESPACE);

        // Finally check a new client (signed by new client key) can consume
        String bobUserName = "bob";
        resources().tlsUser(CLUSTER_NAME, bobUserName).done();
        waitFor("Bob's secret to exist", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_GET_SECRETS,
            () -> KUBE_CLIENT.getSecret(bobUserName) != null,
            () -> LOGGER.error("Couldn't find user secret {}", KUBE_CLIENT.listSecrets()));

        waitForClusterAvailabilityTls(bobUserName, NAMESPACE);
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
        waitTillSecretExists(userName);

        waitForClusterAvailabilityTls(userName, NAMESPACE);

        // Wait until the certificates have been replaced
        waitForCertToChange(clusterCaCert, clusterCaCertificateSecretName(CLUSTER_NAME));

        // Wait until the pods are all up and ready
        waitForClusterStability();

        waitForClusterAvailabilityTls(userName, NAMESPACE);
    }

    private void waitForClusterStability() {
        LOGGER.info("Waiting for cluster stability");
        Map<String, String>[] zkPods = new Map[1];
        Map<String, String>[] kafkaPods = new Map[1];
        Map<String, String>[] eoPods = new Map[1];
        AtomicInteger count = new AtomicInteger();
        zkPods[0] = StUtils.ssSnapshot(NAMESPACE, zookeeperStatefulSetName(CLUSTER_NAME));
        kafkaPods[0] = StUtils.ssSnapshot(NAMESPACE, kafkaStatefulSetName(CLUSTER_NAME));
        eoPods[0] = StUtils.depSnapshot(NAMESPACE, entityOperatorDeploymentName(CLUSTER_NAME));
        TestUtils.waitFor("Cluster stable and ready", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_CLUSTER_STABLE, () -> {
            Map<String, String> zkSnapshot = StUtils.ssSnapshot(NAMESPACE, zookeeperStatefulSetName(CLUSTER_NAME));
            Map<String, String> kafkaSnaptop = StUtils.ssSnapshot(NAMESPACE, kafkaStatefulSetName(CLUSTER_NAME));
            Map<String, String> eoSnapshot = StUtils.depSnapshot(NAMESPACE, entityOperatorDeploymentName(CLUSTER_NAME));
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
            Secret secret = KUBE_CLIENT.getSecret(secretName);
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
        KUBE_CLIENT.createSecret(secret);
        return certAsString;
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
        testClassResources.clusterOperator(NAMESPACE).done();
    }

    @Override
    void tearDownEnvironmentAfterEach() throws Exception {
        deleteTestMethodResources();
        waitForDeletion(Constants.TIMEOUT_TEARDOWN, NAMESPACE);
    }

}
