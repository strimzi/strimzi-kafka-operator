/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Pod;
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
import io.strimzi.systemtest.kafkaclients.externalClients.BasicExternalKafkaClient;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMakerResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.security.SystemTestCertManager;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.systemtest.utils.specific.MetricsUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.WaitException;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import kafka.tools.MirrorMaker;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.strimzi.api.kafka.model.KafkaResources.clientsCaCertificateSecretName;
import static io.strimzi.api.kafka.model.KafkaResources.clientsCaKeySecretName;
import static io.strimzi.api.kafka.model.KafkaResources.clusterCaCertificateSecretName;
import static io.strimzi.api.kafka.model.KafkaResources.clusterCaKeySecretName;
import static io.strimzi.api.kafka.model.KafkaResources.kafkaStatefulSetName;
import static io.strimzi.api.kafka.model.KafkaResources.zookeeperStatefulSetName;
import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.NETWORKPOLICIES_SUPPORTED;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(REGRESSION)
class SecurityST extends BaseST {

    public static final String NAMESPACE = "security-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(SecurityST.class);
    private static final String OPENSSL_RETURN_CODE = "Verify return code: 0 (ok)";
    private static final String TLS_PROTOCOL = "Protocol  : TLSv1";
    private static final String SSL_TIMEOUT = "Timeout   : 300 (sec)";
    private static final String TOPIC_NAME = "test-topic";

    @Test
    void testCertificates() {
        LOGGER.info("Running testCertificates {}", CLUSTER_NAME);
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 2)
                .editSpec()
                    .editZookeeper()
                        .withReplicas(2)
                    .endZookeeper()
                .endSpec()
                .done();

        LOGGER.info("Check Kafka bootstrap certificate");
        String outputCertificate = SystemTestCertManager.generateOpenSslCommandByComponent(KafkaResources.tlsBootstrapAddress(CLUSTER_NAME), KafkaResources.bootstrapServiceName(CLUSTER_NAME),
                KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "kafka", false);
        verifyCerts(outputCertificate, "kafka");

        LOGGER.info("Check zookeeper client certificate");
        outputCertificate = SystemTestCertManager.generateOpenSslCommandByComponent(KafkaResources.zookeeperServiceName(CLUSTER_NAME) + ":2181", KafkaResources.zookeeperServiceName(CLUSTER_NAME),
                KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "kafka");
        verifyCerts(outputCertificate, "zookeeper");

        List<String> kafkaPorts = new ArrayList<>(asList("9091", "9093"));
        List<String> zkPorts = new ArrayList<>(asList("2181", "2888", "3888"));

        IntStream.rangeClosed(0, 1).forEach(podId -> {
            String output;

            LOGGER.info("Checking certificates for podId {}", podId);
            for (String kafkaPort : kafkaPorts) {
                LOGGER.info("Check kafka certificate for port {}", kafkaPort);
                output = SystemTestCertManager.generateOpenSslCommandByComponent(KafkaResources.kafkaPodName(CLUSTER_NAME, podId),
                        KafkaResources.brokersServiceName(CLUSTER_NAME), kafkaPort, "kafka", NAMESPACE);
                verifyCerts(output, "kafka");
            }

            for (String zkPort : zkPorts) {
                try {
                    LOGGER.info("Check zookeeper certificate for port {}", zkPort);
                    output = SystemTestCertManager.generateOpenSslCommandByComponent(KafkaResources.zookeeperPodName(CLUSTER_NAME, podId),
                            KafkaResources.zookeeperHeadlessServiceName(CLUSTER_NAME), zkPort, "zookeeper", NAMESPACE);
                    verifyCerts(output, "zookeeper");
                } catch (KubeClusterException e) {
                    if (e.result != null && e.result.returnCode() == 104) {
                        LOGGER.info("The connection for {} was forcibly closed because of new zookeeper leader", KafkaResources.zookeeperPodName(CLUSTER_NAME, podId));
                    } else {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
    }

    private static void verifyCerts(String certificate, String component) {
        List<String> certificateChains = SystemTestCertManager.getCertificateChain(CLUSTER_NAME + "-" + component);

        assertThat(certificate, containsString(certificateChains.get(0)));
        assertThat(certificate, containsString(certificateChains.get(1)));
        assertThat(certificate, containsString(TLS_PROTOCOL));
        assertThat(certificate, containsString(SSL_TIMEOUT));
        assertThat(certificate, containsString(OPENSSL_RETURN_CODE));
    }

    void autoRenewSomeCaCertsTriggeredByAnno(
            final List<String> secretsToAnnotate,
            boolean zkShouldRoll,
            boolean kafkaShouldRoll,
            boolean eoShouldRoll) {
        String topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);

        createKafkaCluster();

        KafkaUser user = KafkaUserResource.tlsUser(CLUSTER_NAME, USER_NAME).done();
        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, user).done();

        String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(defaultKafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroupName(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE))
            .build();

        LOGGER.info("Checking produced and consumed messages to pod:{}", defaultKafkaClientsPodName);

        internalKafkaClient.checkProducedAndConsumedMessages(
                internalKafkaClient.sendMessagesPlain(),
                internalKafkaClient.receiveMessagesPlain()
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

        internalKafkaClient.setConsumerGroup(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE));

        LOGGER.info("Checking consumed messages to pod:{}", defaultKafkaClientsPodName);

        internalKafkaClient.checkProducedAndConsumedMessages(
                MESSAGE_COUNT,
                internalKafkaClient.receiveMessagesPlain()
        );

        // Check a new client (signed by new client key) can consume
        String bobUserName = "bob";
        user = KafkaUserResource.tlsUser(CLUSTER_NAME, bobUserName).done();
        SecretUtils.waitForSecretReady(bobUserName);
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, user).done();

        defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        internalKafkaClient.setPodName(defaultKafkaClientsPodName);
        internalKafkaClient.setConsumerGroup(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE));

        LOGGER.info("Checking consumed messages to pod:{}", MirrorMaker.defaultMirrorMakerMessageHandler$::new);
        internalKafkaClient.checkProducedAndConsumedMessages(
                MESSAGE_COUNT,
                internalKafkaClient.receiveMessagesPlain()
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
    @Tag(INTERNAL_CLIENTS_USED)
    void testAutoRenewClusterCaCertsTriggeredByAnno() {
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
    @Tag(INTERNAL_CLIENTS_USED)
    void testAutoRenewClientsCaCertsTriggeredByAnno() {
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
    @Tag(INTERNAL_CLIENTS_USED)
    void testAutoRenewAllCaCertsTriggeredByAnno() {
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
                                            boolean eoShouldRoll) {
        createKafkaCluster();

        String aliceUserName = "alice";
        KafkaUser user = KafkaUserResource.tlsUser(CLUSTER_NAME, aliceUserName).done();
        String topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);

        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, user).done();

        SecretUtils.waitForSecretReady(aliceUserName);

        String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(defaultKafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroupName(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE))
            .build();

        LOGGER.info("Checking produced and consumed messages to pod:{}", defaultKafkaClientsPodName);

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
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

        LOGGER.info("Checking consumed messages to pod:{}", defaultKafkaClientsPodName);
        internalKafkaClient.setConsumerGroup(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE));

        internalKafkaClient.checkProducedAndConsumedMessages(
            MESSAGE_COUNT,
            internalKafkaClient.receiveMessagesPlain()
        );

        // Finally check a new client (signed by new client key) can consume
        String bobUserName = "bob";
        user = KafkaUserResource.tlsUser(CLUSTER_NAME, bobUserName).done();
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, user).done();

        defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        internalKafkaClient.setConsumerGroup(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE));
        internalKafkaClient.setPodName(defaultKafkaClientsPodName);

        LOGGER.info("Checking consumed messages to pod:{}", defaultKafkaClientsPodName);

        internalKafkaClient.checkProducedAndConsumedMessages(
            MESSAGE_COUNT,
            internalKafkaClient.receiveMessagesPlain()
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
    @Tag(INTERNAL_CLIENTS_USED)
    void testAutoReplaceClusterCaKeysTriggeredByAnno() {
        autoReplaceSomeKeysTriggeredByAnno(asList(clusterCaKeySecretName(CLUSTER_NAME)),
                true,
                true,
                true);
    }

    @Test
    @Tag(INTERNAL_CLIENTS_USED)
    void testAutoReplaceClientsCaKeysTriggeredByAnno() {
        autoReplaceSomeKeysTriggeredByAnno(asList(clientsCaKeySecretName(CLUSTER_NAME)),
                false,
                true,
                true);
    }

    @Test
    @Tag(INTERNAL_CLIENTS_USED)
    void testAutoReplaceAllCaKeysTriggeredByAnno() {
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
    @Tag(INTERNAL_CLIENTS_USED)
    void testAutoRenewCaCertsTriggerByExpiredCertificate() {
        // 1. Create the Secrets already, and a certificate that's already expired
        String clusterCaCert = TestUtils.readResource(getClass(), "cluster-ca.crt");
        SecretUtils.createSecret(clusterCaCertificateSecretName(CLUSTER_NAME), "ca.crt", new String(Base64.getEncoder().encode(clusterCaCert.getBytes()), StandardCharsets.US_ASCII));
        String topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);

        // 2. Now create a cluster
        createKafkaCluster();
        String userName = "alice";
        KafkaUser user = KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();

        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, user).done();

        String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(defaultKafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroupName(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE))
            .build();

        // Check if user exists
        SecretUtils.waitForSecretReady(userName);

        LOGGER.info("Checking produced and consumed messages to pod:{}", defaultKafkaClientsPodName);
        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

        // Wait until the certificates have been replaced
        SecretUtils.waitForCertToChange(clusterCaCert, clusterCaCertificateSecretName(CLUSTER_NAME));

        // Wait until the pods are all up and ready
        KafkaUtils.waitForClusterStability(CLUSTER_NAME);

        LOGGER.info("Checking produced and consumed messages to pod:{}", defaultKafkaClientsPodName);

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );
    }

    @Test
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

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(defaultKafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(user.getMetadata().getName())
            .withConsumerGroupName(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE))
            .build();

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

        LOGGER.info("Checking consumed messages to pod:{}", defaultKafkaClientsPodName);

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(),
            internalKafkaClient.receiveMessagesTls()
        );
    }

    @Test
    @Tag(INTERNAL_CLIENTS_USED)
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

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(defaultKafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(userName)
            .withConsumerGroupName(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE))
            .build();

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

        PodUtils.waitUntilPodsStability(kubeClient().listPodsByPrefixInName(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME)));
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

        LOGGER.info("Checking consumed messages to pod:{}", defaultKafkaClientsPodName);

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(),
            internalKafkaClient.receiveMessagesTls()
        );

    }

    @Test
    @Tag(NETWORKPOLICIES_SUPPORTED)
    @Tag(INTERNAL_CLIENTS_USED)
    void testNetworkPoliciesWithPlainListener() {
        String allowedKafkaClientsName = CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS + "-allow";
        String deniedKafkaClientsName = CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS + "-deny";
        Map<String, String> matchLabelForPlain = new HashMap<>();
        matchLabelForPlain.put("app", allowedKafkaClientsName);

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 1, 1)
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

        KafkaClientsResource.deployKafkaClients(false, allowedKafkaClientsName, kafkaUser).done();

        String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(allowedKafkaClientsName).get(0).getMetadata().getName();

        LOGGER.info("Verifying that {} pod is able to exchange messages", kafkaClientsPodName);

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(topic0)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(userName)
            .withConsumerGroupName(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE))
            .build();

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

        KafkaClientsResource.deployKafkaClients(false, deniedKafkaClientsName, kafkaUser).done();

        String kafkaClientsNewPodName = kubeClient().listPodsByPrefixInName(deniedKafkaClientsName).get(0).getMetadata().getName();

        internalKafkaClient.setPodName(kafkaClientsNewPodName);
        internalKafkaClient.setTopicName(topic1);
        internalKafkaClient.setConsumerGroup(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE));


        LOGGER.info("Verifying that {} pod is not able to exchange messages", kafkaClientsNewPodName);
        assertThrows(RuntimeException.class, () ->  {
            internalKafkaClient.checkProducedAndConsumedMessages(
                internalKafkaClient.sendMessagesPlain(),
                internalKafkaClient.receiveMessagesPlain()
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
    @Tag(INTERNAL_CLIENTS_USED)
    void testNetworkPoliciesWithTlsListener() {
        String allowedKafkaClientsName = CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS + "-allow";
        String deniedKafkaClientsName = CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS + "-deny";
        Map<String, String> matchLabelsForTls = new HashMap<>();
        matchLabelsForTls.put("app", allowedKafkaClientsName);

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 1, 1)
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

        KafkaClientsResource.deployKafkaClients(true, allowedKafkaClientsName, kafkaUser).done();

        String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(allowedKafkaClientsName).get(0).getMetadata().getName();

        LOGGER.info("Verifying that {} pod is able to exchange messages", kafkaClientsPodName);

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(topic0)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(userName)
            .withConsumerGroupName(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE))
            .build();

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(),
            internalKafkaClient.receiveMessagesTls()
        );

        KafkaClientsResource.deployKafkaClients(true, deniedKafkaClientsName, kafkaUser).done();

        String kafkaClientsNewPodName = kubeClient().listPodsByPrefixInName(deniedKafkaClientsName).get(0).getMetadata().getName();

        internalKafkaClient.setPodName(kafkaClientsNewPodName);
        internalKafkaClient.setConsumerGroup(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE));
        internalKafkaClient.setTopicName(topic1);

        LOGGER.info("Verifying that {} pod is  not able to exchange messages", kafkaClientsNewPodName);

        assertThrows(RuntimeException.class, () -> {
            internalKafkaClient.checkProducedAndConsumedMessages(
                internalKafkaClient.sendMessagesTls(),
                internalKafkaClient.receiveMessagesTls()
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
    @Tag(EXTERNAL_CLIENTS_USED)
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

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withKafkaUsername(kafkaUserWrite)
            .withMessageCount(numberOfMessages)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withConsumerGroupName(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE))
            .build();

        assertThat(basicExternalKafkaClient.sendMessagesTls(), is(numberOfMessages));

        assertThrows(WaitException.class, () -> basicExternalKafkaClient.receiveMessagesTls());

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
                        .withOperation(AclOperation.DESCRIBE)  //s describe is for that user can find out metadata
                    .endAcl()
                .endKafkaUserAuthorizationSimple()
            .endSpec()
            .done();

        SecretUtils.waitForSecretReady(kafkaUserRead);


        basicExternalKafkaClient.setConsumerGroup(consumerGroupName);
        basicExternalKafkaClient.setKafkaUsername(kafkaUserRead);

        assertThat(basicExternalKafkaClient.receiveMessagesTls(), is(numberOfMessages));

        LOGGER.info("Checking kafka user:{} that is not able to send messages to topic:{}", kafkaUserRead, topicName);
        assertThrows(WaitException.class, () -> basicExternalKafkaClient.sendMessagesTls());
    }

    @Test
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
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

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withKafkaUsername(USER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withConsumerGroupName(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE))
            .build();

        assertThat(basicExternalKafkaClient.sendMessagesTls(), is(MESSAGE_COUNT));

        LOGGER.info("Checking kafka super user:{} that is able to read messages to topic:{} regardless that " +
                "we configured Acls with only write operation", USER_NAME, TOPIC_NAME);

        assertThat(basicExternalKafkaClient.receiveMessagesTls(), is(MESSAGE_COUNT));

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

        basicExternalKafkaClient.setKafkaUsername(nonSuperuserName);

        assertThat(basicExternalKafkaClient.sendMessagesTls(), is(MESSAGE_COUNT));

        LOGGER.info("Checking kafka super user:{} that is not able to read messages to topic:{} because of defined" +
                " ACLs on only write operation", nonSuperuserName, TOPIC_NAME);

        basicExternalKafkaClient.setConsumerGroup(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE));

        assertThrows(WaitException.class, () -> basicExternalKafkaClient.receiveMessagesTls(Constants.GLOBAL_CLIENTS_EXCEPT_ERROR_TIMEOUT));
    }

    @Test
    @Tag(INTERNAL_CLIENTS_USED)
    void testCaRenewalBreakInMiddle() {
        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3, 3)
            .editSpec()
                .withNewClusterCa()
                    .withRenewalDays(1)
                    .withValidityDays(3)
                .endClusterCa()
            .endSpec().done();

        KafkaUtils.waitUntilKafkaStatus(CLUSTER_NAME, "Ready");

        KafkaUser user = KafkaUserResource.tlsUser(CLUSTER_NAME, USER_NAME).done();
        String topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);

        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, user).done();

        String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(defaultKafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withKafkaUsername(USER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroupName(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE))
            .build();

        internalKafkaClient.setPodName(defaultKafkaClientsPodName);

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(),
            internalKafkaClient.receiveMessagesTls()
        );

        Map<String, String> zkPods = StatefulSetUtils.ssSnapshot(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME));
        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));
        Map<String, String> eoPods = DeploymentUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME));

        String clusterCaCert = TestUtils.readResource(getClass(), "cluster-ca.crt");
        SecretUtils.createSecret(clusterCaCertificateSecretName(CLUSTER_NAME), "ca.crt", new String(Base64.getEncoder().encode(clusterCaCert.getBytes()), StandardCharsets.US_ASCII));

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

        TestUtils.waitFor("Waiting for some kafka pod to be in the pending phase because of selected high cpu resource",
            Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> {
                List<Pod> filteredPod = kubeClient().listPodsByPrefixInName(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME))
                    .stream().filter(pod -> pod.getStatus().getPhase().equals("Pending")).collect(Collectors.toList());
                LOGGER.info("Filtered pods are {}", filteredPod.toString());
                return filteredPod.get(0).getStatus().getPhase().equals("Pending");
            }
        );

        internalKafkaClient.setConsumerGroup(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE));

        int received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(MESSAGE_COUNT));

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec()
                .getZookeeper()
                .setResources(new ResourceRequirementsBuilder()
                    .addToRequests("cpu", new Quantity("200m"))
                    .build());
        });

        // Wait until the certificates have been replaced
        SecretUtils.waitForCertToChange(clusterCaCert, KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME));
        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), 3, zkPods);
        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);
        DeploymentUtils.waitTillDepHasRolled(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), 1, eoPods);

        internalKafkaClient.setConsumerGroup(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE));

        LOGGER.info("Checking produced and consumed messages to pod:{}", internalKafkaClient.getPodName());
        received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(MESSAGE_COUNT));

        // Try to send and receive messages with new certificates
        topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);

        internalKafkaClient.setConsumerGroup(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE));
        internalKafkaClient.setTopicName(topicName);

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(),
            internalKafkaClient.receiveMessagesTls()
        );
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
