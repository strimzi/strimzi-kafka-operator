/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.strimzi.api.kafka.model.AclOperation;
import io.strimzi.api.kafka.model.CertificateAuthority;
import io.strimzi.api.kafka.model.CertificateAuthorityBuilder;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMakerResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.externalClients.BasicExternalKafkaClient;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMakerResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMakerTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMakerUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.WaitException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.ROLLING_UPDATE;
import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.security.SystemTestCertManager.STRIMZI_INTERMEDIATE_CA;
import static io.strimzi.systemtest.security.SystemTestCertManager.convertPrivateKeyToPKCS8File;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(REGRESSION)
class SecurityST extends AbstractST {

    public static final String NAMESPACE = "security-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(SecurityST.class);
    private static final String OPENSSL_RETURN_CODE = "Verify return code: 0 (ok)";
    private static final String TLS_PROTOCOL = "Protocol  : TLSv1";
    private static final String SSL_TIMEOUT = "Timeout   : 300 (sec)";
    static final String STRIMZI_TEST_CLUSTER_CA = "C=CZ, L=Prague, O=StrimziTest, CN=SecuritySTClusterCA";
    static final String STRIMZI_TEST_CLIENTS_CA = "C=CZ, L=Prague, O=StrimziTest, CN=SecuritySTClientsCA";

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testCertificates(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        LOGGER.info("Running testCertificates {}", clusterName);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 2)
                .editSpec()
                    .editZookeeper()
                        .withReplicas(2)
                    .endZookeeper()
                .endSpec()
                .build());

        LOGGER.info("Check Kafka bootstrap certificate");
        String outputCertificate = SystemTestCertManager.generateOpenSslCommandByComponent(KafkaResources.tlsBootstrapAddress(clusterName), KafkaResources.bootstrapServiceName(clusterName),
                KafkaResources.kafkaPodName(clusterName, 0), "kafka", false);
        verifyCerts(clusterName, outputCertificate, "kafka");

        LOGGER.info("Check zookeeper client certificate");
        outputCertificate = SystemTestCertManager.generateOpenSslCommandByComponent(KafkaResources.zookeeperServiceName(clusterName) + ":2181", KafkaResources.zookeeperServiceName(clusterName),
                KafkaResources.kafkaPodName(clusterName, 0), "kafka");
        verifyCerts(clusterName, outputCertificate, "zookeeper");

        List<String> kafkaPorts = new ArrayList<>(Arrays.asList("9091", "9093"));
        List<String> zkPorts = new ArrayList<>(Arrays.asList("2181", "3888"));

        IntStream.rangeClosed(0, 1).forEach(podId -> {
            String output;

            LOGGER.info("Checking certificates for podId {}", podId);
            for (String kafkaPort : kafkaPorts) {
                LOGGER.info("Check kafka certificate for port {}", kafkaPort);
                output = SystemTestCertManager.generateOpenSslCommandByComponent(KafkaResources.kafkaPodName(clusterName, podId),
                        KafkaResources.brokersServiceName(clusterName), kafkaPort, "kafka", NAMESPACE);
                verifyCerts(clusterName, output, "kafka");
            }

            for (String zkPort : zkPorts) {
                LOGGER.info("Check zookeeper certificate for port {}", zkPort);
                output = SystemTestCertManager.generateOpenSslCommandByComponent(KafkaResources.zookeeperPodName(clusterName, podId),
                        KafkaResources.zookeeperHeadlessServiceName(clusterName), zkPort, "zookeeper", NAMESPACE);
                verifyCerts(clusterName, output, "zookeeper");
            }
        });
    }

    // synchronized avoiding data-race (list of string is allocated on the heap), but has different reference on stack it's ok
    // but Strings parameters provided are not created in scope of this method
    synchronized private static void verifyCerts(String clusterName, String certificate, String component) {
        List<String> certificateChains = SystemTestCertManager.getCertificateChain(clusterName + "-" + component);

        assertThat(certificate, containsString(certificateChains.get(0)));
        assertThat(certificate, containsString(certificateChains.get(1)));
        assertThat(certificate, containsString(TLS_PROTOCOL));
        assertThat(certificate, containsString(SSL_TIMEOUT));
        assertThat(certificate, containsString(OPENSSL_RETURN_CODE));
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(INTERNAL_CLIENTS_USED)
    @Tag(ROLLING_UPDATE)
    @Tag("ClusterCaCerts")
    void testAutoRenewClusterCaCertsTriggeredByAnno(ExtensionContext extensionContext) {
        autoRenewSomeCaCertsTriggeredByAnno(
                extensionContext,
                /* ZK node need new certs */
                true,
                /* brokers need new certs */
                true,
                /* eo needs new cert */
                true);
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(INTERNAL_CLIENTS_USED)
    @Tag(ROLLING_UPDATE)
    @Tag("ClientsCaCerts")
    void testAutoRenewClientsCaCertsTriggeredByAnno(ExtensionContext extensionContext) {
        autoRenewSomeCaCertsTriggeredByAnno(
            extensionContext,
                /* no communication between clients and zk, so no need to roll */
                false,
                /* brokers need to trust client certs with new cert */
                true,
                /* eo needs to generate new client certs */
                true);
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(ACCEPTANCE)
    @Tag(INTERNAL_CLIENTS_USED)
    @Tag(ROLLING_UPDATE)
    @Tag("AllCaCerts")
    void testAutoRenewAllCaCertsTriggeredByAnno(ExtensionContext extensionContext) {
        autoRenewSomeCaCertsTriggeredByAnno(
            extensionContext,
                true,
                true,
                true);
    }

    @SuppressWarnings({"checkstyle:MethodLength", "checkstyle:NPathComplexity"})
    void autoRenewSomeCaCertsTriggeredByAnno(
            ExtensionContext extensionContext,
            boolean zkShouldRoll,
            boolean kafkaShouldRoll,
            boolean eoShouldRoll) {

        createKafkaCluster(extensionContext);

        String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        List<String> secrets = null;

        // to make it parallel we need decision maker...
        if (extensionContext.getTags().contains("ClusterCaCerts")) {
            secrets = Arrays.asList(clusterCaCertificateSecretName(clusterName));
        } else if (extensionContext.getTags().contains("ClientsCaCerts")) {
            secrets = Arrays.asList(clientsCaCertificateSecretName(clusterName));
        } else {
            // AllCaKeys
            secrets = Arrays.asList(clusterCaCertificateSecretName(clusterName),
                clientsCaCertificateSecretName(clusterName));
        }

        KafkaUser user = KafkaUserTemplates.tlsUser(clusterName, userName).build();

        resourceManager.createResource(extensionContext, user);
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, kafkaClientsName, user).build());

        String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(kafkaClientsName).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(defaultKafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        LOGGER.info("Checking produced and consumed messages to pod:{}", defaultKafkaClientsPodName);

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

        // Get all pods, and their resource versions
        Map<String, String> zkPods = StatefulSetUtils.ssSnapshot(zookeeperStatefulSetName(clusterName));
        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(clusterName));
        Map<String, String> eoPod = DeploymentUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(clusterName));

        LOGGER.info("Triggering CA cert renewal by adding the annotation");
        Map<String, String> initialCaCerts = new HashMap<>();
        for (String secretName : secrets) {
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
            StatefulSetUtils.waitTillSsHasRolled(zookeeperStatefulSetName(clusterName), 3, zkPods);
        }
        if (kafkaShouldRoll) {
            LOGGER.info("Wait for kafka to rolling restart ...");
            StatefulSetUtils.waitTillSsHasRolled(kafkaStatefulSetName(clusterName), 3, kafkaPods);
        }
        if (eoShouldRoll) {
            LOGGER.info("Wait for EO to rolling restart ...");
            eoPod = DeploymentUtils.waitTillDepHasRolled(KafkaResources.entityOperatorDeploymentName(clusterName), 1, eoPod);
        }

        LOGGER.info("Checking the certificates have been replaced");
        for (String secretName : secrets) {
            Secret secret = kubeClient().getSecret(secretName);
            assertThat("Secret " + secretName + " should exist", secret, is(notNullValue()));
            assertThat("CA cert in " + secretName + " should have non-null 'data'", is(notNullValue()));
            String value = secret.getData().get("ca.crt");
            assertThat("CA cert in " + secretName + " should have changed",
                value, is(not(initialCaCerts.get(secretName))));
        }

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withConsumerGroupName(ClientUtils.generateRandomConsumerGroup())
            .build();

        LOGGER.info("Checking consumed messages to pod:{}", defaultKafkaClientsPodName);

        internalKafkaClient.checkProducedAndConsumedMessages(
            MESSAGE_COUNT,
            internalKafkaClient.receiveMessagesPlain()
        );

        // Check a new client (signed by new client key) can consume
        String bobUserName = "bob-" + clusterName;

        user = KafkaUserTemplates.tlsUser(clusterName, bobUserName).build();

        resourceManager.createResource(extensionContext, user);
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, kafkaClientsName + "-tls", user).build());

        defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(kafkaClientsName + "-tls").get(0).getMetadata().getName();

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withConsumerGroupName(ClientUtils.generateRandomConsumerGroup())
            .withUsingPodName(defaultKafkaClientsPodName)
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .withKafkaUsername(bobUserName)
            .build();

        LOGGER.info("Checking consumed messages to pod:{}", defaultKafkaClientsPodName);

        internalKafkaClient.checkProducedAndConsumedMessages(
            MESSAGE_COUNT,
            internalKafkaClient.receiveMessagesPlain()
        );

        if (!zkShouldRoll) {
            assertThat("ZK pods should not roll, but did.", StatefulSetUtils.ssSnapshot(zookeeperStatefulSetName(clusterName)), is(zkPods));
        }
        if (!kafkaShouldRoll) {
            assertThat("Kafka pods should not roll, but did.", StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(clusterName)), is(kafkaPods));
        }
        if (!eoShouldRoll) {
            assertThat("EO pod should not roll, but did.", DeploymentUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(clusterName)), is(eoPod));
        }
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(INTERNAL_CLIENTS_USED)
    @Tag(ROLLING_UPDATE)
    @Tag("ClusterCaKeys")
    void testAutoReplaceClusterCaKeysTriggeredByAnno(ExtensionContext extensionContext) {
        autoReplaceSomeKeysTriggeredByAnno(
            extensionContext,
                true,
                true,
                true);
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(INTERNAL_CLIENTS_USED)
    @Tag(ROLLING_UPDATE)
    @Tag("ClientsCaKeys")
    void testAutoReplaceClientsCaKeysTriggeredByAnno(ExtensionContext extensionContext) {
        autoReplaceSomeKeysTriggeredByAnno(
            extensionContext,
                false,
                true,
                true);
    }

    @ParallelTest
    @Tag(INTERNAL_CLIENTS_USED)
    @Tag(ROLLING_UPDATE)
    @Tag("AllCaKeys")
    void testAutoReplaceAllCaKeysTriggeredByAnno(ExtensionContext extensionContext) {
        autoReplaceSomeKeysTriggeredByAnno(
            extensionContext,
                true,
                true,
                true);
    }

    @SuppressWarnings({"checkstyle:MethodLength", "checkstyle:NPathComplexity"})
    void autoReplaceSomeKeysTriggeredByAnno(ExtensionContext extensionContext,
                                            boolean zkShouldRoll,
                                            boolean kafkaShouldRoll,
                                            boolean eoShouldRoll) {


        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());
        List<String> secrets = null;

        // to make it parallel we need decision maker...
        if (extensionContext.getTags().contains("ClusterCaKeys")) {
            secrets = Arrays.asList(clusterCaKeySecretName(clusterName));
        } else if (extensionContext.getTags().contains("ClientsCaKeys")) {
            secrets = Arrays.asList(clientsCaKeySecretName(clusterName));
        } else {
            // AllCaKeys
            secrets = Arrays.asList(clusterCaKeySecretName(clusterName),
                clientsCaKeySecretName(clusterName));
        }

        createKafkaCluster(extensionContext);

        KafkaUser user = KafkaUserTemplates.tlsUser(clusterName, KafkaUserUtils.generateRandomNameOfKafkaUser()).build();

        resourceManager.createResource(extensionContext, user);

        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, kafkaClientsName, user).build());

        String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(kafkaClientsName).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(defaultKafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        LOGGER.info("Checking produced and consumed messages to pod:{}", defaultKafkaClientsPodName);

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

        // Get all pods, and their resource versions
        Map<String, String> zkPods = StatefulSetUtils.ssSnapshot(zookeeperStatefulSetName(clusterName));
        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(clusterName));
        Map<String, String> eoPod = DeploymentUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(clusterName));

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
            zkPods = StatefulSetUtils.waitTillSsHasRolled(zookeeperStatefulSetName(clusterName), 3, zkPods);
        }
        if (kafkaShouldRoll) {
            LOGGER.info("Wait for kafka to rolling restart (1)...");
            kafkaPods = StatefulSetUtils.waitTillSsHasRolled(kafkaStatefulSetName(clusterName), kafkaPods);
        }
        if (eoShouldRoll) {
            LOGGER.info("Wait for EO to rolling restart (1)...");
            eoPod = DeploymentUtils.waitTillDepHasRolled(KafkaResources.entityOperatorDeploymentName(clusterName), 1, eoPod);
        }

        if (zkShouldRoll) {
            LOGGER.info("Wait for zk to rolling restart (2)...");
            zkPods = StatefulSetUtils.waitTillSsHasRolled(zookeeperStatefulSetName(clusterName), 3, zkPods);
        }
        if (kafkaShouldRoll) {
            LOGGER.info("Wait for kafka to rolling restart (2)...");
            kafkaPods = StatefulSetUtils.waitTillSsHasRolled(kafkaStatefulSetName(clusterName), 3, kafkaPods);
        }

        if (eoShouldRoll) {
            LOGGER.info("Wait for EO to rolling restart (2)...");
            eoPod = DeploymentUtils.waitTillDepHasRolled(KafkaResources.entityOperatorDeploymentName(clusterName), 1, eoPod);
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

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withConsumerGroupName(ClientUtils.generateRandomConsumerGroup())
            .build();

        internalKafkaClient.checkProducedAndConsumedMessages(
            MESSAGE_COUNT,
            internalKafkaClient.receiveMessagesPlain()
        );

        // Finally check a new client (signed by new client key) can consume

        user = KafkaUserTemplates.tlsUser(clusterName, KafkaUserUtils.generateRandomNameOfKafkaUser()).build();

        resourceManager.createResource(extensionContext, user);
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, kafkaClientsName + "-tls", user).build());

        defaultKafkaClientsPodName =
            ResourceManager.kubeClient().listPodsByPrefixInName(kafkaClientsName + "-tls").get(0).getMetadata().getName();

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withUsingPodName(defaultKafkaClientsPodName)
            .withConsumerGroupName(ClientUtils.generateRandomConsumerGroup())
            .build();

        LOGGER.info("Checking consumed messages to pod:{}", defaultKafkaClientsPodName);

        internalKafkaClient.checkProducedAndConsumedMessages(
            MESSAGE_COUNT,
            internalKafkaClient.receiveMessagesPlain()
        );

        if (!zkShouldRoll) {
            assertThat("ZK pods should not roll, but did.", StatefulSetUtils.ssSnapshot(zookeeperStatefulSetName(clusterName)), is(zkPods));
        }

        if (!kafkaShouldRoll) {
            assertThat("Kafka pods should not roll, but did.", StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(clusterName)), is(kafkaPods));

        }

        if (!eoShouldRoll) {
            assertThat("EO pod should not roll, but did.", DeploymentUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(clusterName)), is(eoPod));
        }
    }

    private void createKafkaCluster(ExtensionContext extensionContext) {
        LOGGER.info("Creating a cluster");
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
                            .withPort(9092)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(false)
                        .endGenericKafkaListener()
                        .addNewGenericKafkaListener()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewKafkaListenerAuthenticationTlsAuth()
                            .endKafkaListenerAuthenticationTlsAuth()
                        .endGenericKafkaListener()
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
            .build());
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(INTERNAL_CLIENTS_USED)
    void testAutoRenewCaCertsTriggerByExpiredCertificate(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());

        // 1. Create the Secrets already, and a certificate that's already expired
        InputStream secretInputStream = getClass().getClassLoader().getResourceAsStream("security-st-certs/expired-cluster-ca.crt");
        String clusterCaCert = TestUtils.readResource(secretInputStream);
        SecretUtils.createSecret(clusterCaCertificateSecretName(clusterName), "ca.crt", new String(Base64.getEncoder().encode(clusterCaCert.getBytes()), StandardCharsets.US_ASCII));

        // 2. Now create a cluster
        createKafkaCluster(extensionContext);

        KafkaUser user = KafkaUserTemplates.tlsUser(clusterName, userName).build();

        resourceManager.createResource(extensionContext, user);
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, user).build());

        String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(defaultKafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        LOGGER.info("Checking produced and consumed messages to pod:{}", defaultKafkaClientsPodName);
        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

        // Wait until the certificates have been replaced
        SecretUtils.waitForCertToChange(clusterCaCert, clusterCaCertificateSecretName(clusterName));

        // Wait until the pods are all up and ready
        KafkaUtils.waitForClusterStability(clusterName);

        LOGGER.info("Checking produced and consumed messages to pod:{}", defaultKafkaClientsPodName);

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(INTERNAL_CLIENTS_USED)
    void testCertRenewalInMaintenanceWindow(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        String secretName = KafkaResources.clusterCaCertificateSecretName(clusterName);

        LocalDateTime maintenanceWindowStart = LocalDateTime.now().withSecond(0);
        long maintenanceWindowDuration = 14;
        maintenanceWindowStart = maintenanceWindowStart.plusMinutes(5);
        long windowStartMin = maintenanceWindowStart.getMinute();
        long windowStopMin = windowStartMin + maintenanceWindowDuration > 59
                ? windowStartMin + maintenanceWindowDuration - 60 : windowStartMin + maintenanceWindowDuration;

        String maintenanceWindowCron = "* " + windowStartMin + "-" + windowStopMin + " * * * ? *";
        LOGGER.info("Maintenance window is: {}", maintenanceWindowCron);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 1)
            .editSpec()
                .addNewMaintenanceTimeWindow(maintenanceWindowCron)
            .endSpec()
            .build());


        KafkaUser user = KafkaUserTemplates.tlsUser(clusterName, KafkaUserUtils.generateRandomNameOfKafkaUser()).build();

        resourceManager.createResource(extensionContext, user);
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, user).build());

        String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(defaultKafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(user.getMetadata().getName())
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(clusterName));

        LOGGER.info("Annotate secret {} with secret force-renew annotation", secretName);
        Secret secret = new SecretBuilder(kubeClient().getSecret(secretName))
            .editMetadata()
                .addToAnnotations(Ca.ANNO_STRIMZI_IO_FORCE_RENEW, "true")
            .endMetadata().build();
        kubeClient().patchSecret(secretName, secret);

        LOGGER.info("Wait until maintenance windows starts");
        LocalDateTime finalMaintenanceWindowStart = maintenanceWindowStart;
        TestUtils.waitFor("maintenance window start",
            Constants.GLOBAL_POLL_INTERVAL, Duration.ofMinutes(maintenanceWindowDuration).toMillis() - 10000,
            () -> LocalDateTime.now().isAfter(finalMaintenanceWindowStart));

        LOGGER.info("Maintenance window starts");

        assertThat("Rolling update was performed out of maintenance window!", kafkaPods, is(StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(clusterName))));

        LOGGER.info("Wait until rolling update is triggered during maintenance window");
        StatefulSetUtils.waitTillSsHasRolled(kafkaStatefulSetName(clusterName), 3, kafkaPods);

        assertThat("Rolling update wasn't performed in correct time", LocalDateTime.now().isAfter(maintenanceWindowStart));

        LOGGER.info("Checking consumed messages to pod:{}", defaultKafkaClientsPodName);

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(),
            internalKafkaClient.receiveMessagesTls()
        );
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(INTERNAL_CLIENTS_USED)
    void testCertRegeneratedAfterInternalCAisDeleted(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 1).build());

        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(clusterName));

        KafkaUser user = KafkaUserTemplates.tlsUser(clusterName, userName).build();

        resourceManager.createResource(extensionContext, user);
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, user).build());

        String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(defaultKafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(userName)
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        // TODO
        List<Secret> secrets = kubeClient().listSecrets().stream()
            .filter(secret ->
                secret.getMetadata().getName().startsWith(clusterName) &&
                secret.getMetadata().getName().endsWith("ca-cert"))
            .collect(Collectors.toList());

        for (Secret s : secrets) {
            LOGGER.info("Verifying that secret {} with name {} is present", s, s.getMetadata().getName());
            assertThat(s.getData(), is(notNullValue()));
        }

        for (Secret s : secrets) {
            LOGGER.info("Deleting secret {}", s.getMetadata().getName());
            kubeClient().deleteSecret(s.getMetadata().getName());
        }

        PodUtils.verifyThatRunningPodsAreStable(KafkaResources.kafkaStatefulSetName(clusterName));
        StatefulSetUtils.waitTillSsHasRolled(kafkaStatefulSetName(clusterName), 3, kafkaPods);

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

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testTlsHostnameVerificationWithKafkaConnect(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3, 1).build());
        LOGGER.info("Getting IP of the bootstrap service");

        String ipOfBootstrapService = kubeClient().getService(KafkaResources.bootstrapServiceName(clusterName)).getSpec().getClusterIP();

        LOGGER.info("KafkaConnect without config {} will not connect to {}:9093", "ssl.endpoint.identification.algorithm", ipOfBootstrapService);

        resourceManager.createResource(extensionContext,
                KafkaClientsTemplates.kafkaClients(true, kafkaClientsName).build());

        resourceManager.createResource(extensionContext, false, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, clusterName, 1)
            .editSpec()
                .withNewTls()
                    .addNewTrustedCertificate()
                        .withSecretName(KafkaResources.clusterCaCertificateSecretName(clusterName))
                        .withCertificate("ca.crt")
                    .endTrustedCertificate()
                .endTls()
                .withBootstrapServers(ipOfBootstrapService + ":9093")
            .endSpec()
            .build());

        PodUtils.waitUntilPodIsPresent(clusterName + "-connect");

        String kafkaConnectPodName = kubeClient().listPods(clusterName, Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();

        PodUtils.waitUntilPodIsInCrashLoopBackOff(kafkaConnectPodName);

        assertThat("CrashLoopBackOff", is(kubeClient().getPod(kafkaConnectPodName).getStatus().getContainerStatuses()
                .get(0).getState().getWaiting().getReason()));

        KafkaConnectResource.replaceKafkaConnectResource(clusterName, kc -> {
            kc.getSpec().getConfig().put("ssl.endpoint.identification.algorithm", "");
        });

        LOGGER.info("KafkaConnect with config {} will connect to {}:9093", "ssl.endpoint.identification.algorithm", ipOfBootstrapService);

        KafkaConnectUtils.waitForConnectReady(clusterName);

        KafkaMirrorMakerResource.kafkaMirrorMakerClient().inNamespace(NAMESPACE).withName(clusterName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
        DeploymentUtils.waitForDeploymentDeletion(KafkaConnectResources.deploymentName(clusterName));
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testTlsHostnameVerificationWithMirrorMaker(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String sourceKafkaCluster = clusterName + "-source";
        String targetKafkaCluster = clusterName + "-target";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(sourceKafkaCluster, 1, 1).build());
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(targetKafkaCluster, 1, 1).build());

        LOGGER.info("Getting IP of the source bootstrap service for consumer");
        String ipOfSourceBootstrapService = kubeClient().getService(KafkaResources.bootstrapServiceName(sourceKafkaCluster)).getSpec().getClusterIP();

        LOGGER.info("Getting IP of the target bootstrap service for producer");
        String ipOfTargetBootstrapService = kubeClient().getService(KafkaResources.bootstrapServiceName(targetKafkaCluster)).getSpec().getClusterIP();

        LOGGER.info("KafkaMirrorMaker without config {} will not connect to consumer with address {}:9093", "ssl.endpoint.identification.algorithm", ipOfSourceBootstrapService);
        LOGGER.info("KafkaMirrorMaker without config {} will not connect to producer with address {}:9093", "ssl.endpoint.identification.algorithm", ipOfTargetBootstrapService);

        resourceManager.createResource(extensionContext, false, KafkaMirrorMakerTemplates.kafkaMirrorMaker(clusterName, sourceKafkaCluster, targetKafkaCluster,
            ClientUtils.generateRandomConsumerGroup(), 1, true)
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

        PodUtils.waitUntilPodIsPresent(clusterName + "-mirror-maker");

        String kafkaMirrorMakerPodName = kubeClient().listPods(Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker.RESOURCE_KIND).get(0).getMetadata().getName();

        PodUtils.waitUntilPodIsInCrashLoopBackOff(kafkaMirrorMakerPodName);

        assertThat("CrashLoopBackOff", is(kubeClient().getPod(kafkaMirrorMakerPodName).getStatus().getContainerStatuses().get(0)
                .getState().getWaiting().getReason()));

        LOGGER.info("KafkaMirrorMaker with config {} will connect to consumer with address {}:9093", "ssl.endpoint.identification.algorithm", ipOfSourceBootstrapService);
        LOGGER.info("KafkaMirrorMaker with config {} will connect to producer with address {}:9093", "ssl.endpoint.identification.algorithm", ipOfTargetBootstrapService);

        LOGGER.info("Adding configuration {} to the mirror maker...", "ssl.endpoint.identification.algorithm");
        KafkaMirrorMakerResource.replaceMirrorMakerResource(clusterName, mm -> {
            mm.getSpec().getConsumer().getConfig().put("ssl.endpoint.identification.algorithm", ""); // disable hostname verification
            mm.getSpec().getProducer().getConfig().put("ssl.endpoint.identification.algorithm", ""); // disable hostname verification
        });

        KafkaMirrorMakerUtils.waitForKafkaMirrorMakerReady(clusterName);

        KafkaMirrorMakerResource.kafkaMirrorMakerClient().inNamespace(NAMESPACE).withName(clusterName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
        DeploymentUtils.waitForDeploymentDeletion(KafkaMirrorMakerResources.deploymentName(clusterName));
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    void testAclRuleReadAndWrite(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final String kafkaUserWrite = "kafka-user-write";
        final String kafkaUserRead = "kafka-user-read";
        final int numberOfMessages = 500;
        final String consumerGroupName = "consumer-group-name-1";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3, 1)
            .editSpec()
                .editKafka()
                    .withNewKafkaAuthorizationSimple()
                    .endKafkaAuthorizationSimple()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9094)
                            .withType(KafkaListenerType.NODEPORT)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationTls())
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());
        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(clusterName, kafkaUserWrite)
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
            .build());

        LOGGER.info("Checking KafkaUser {} that is able to send messages to topic '{}'", kafkaUserWrite, topicName);

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withKafkaUsername(kafkaUserWrite)
            .withMessageCount(numberOfMessages)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        assertThat(basicExternalKafkaClient.sendMessagesTls(), is(numberOfMessages));

        assertThrows(WaitException.class, basicExternalKafkaClient::receiveMessagesTls);

        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(clusterName, kafkaUserRead)
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
            .build());

        BasicExternalKafkaClient newBasicExternalKafkaClient = basicExternalKafkaClient.toBuilder()
            .withKafkaUsername(kafkaUserRead)
            .withConsumerGroupName(consumerGroupName)
            .build();

        assertThat(newBasicExternalKafkaClient.receiveMessagesTls(), is(numberOfMessages));

        LOGGER.info("Checking KafkaUser {} that is not able to send messages to topic '{}'", kafkaUserRead, topicName);
        assertThrows(WaitException.class, newBasicExternalKafkaClient::sendMessagesTls);
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    void testAclWithSuperUser(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3, 1)
            .editSpec()
                .editKafka()
                    .withNewKafkaAuthorizationSimple()
                        .withSuperUsers("CN=" + userName)
                    .endKafkaAuthorizationSimple()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9094)
                            .withType(KafkaListenerType.NODEPORT)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationTls())
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());
        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(clusterName, userName)
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
            .build());

        LOGGER.info("Checking kafka super user:{} that is able to send messages to topic:{}", userName, topicName);

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        assertThat(basicExternalKafkaClient.sendMessagesTls(), is(MESSAGE_COUNT));

        LOGGER.info("Checking kafka super user:{} that is able to read messages to topic:{} regardless that " +
                "we configured Acls with only write operation", userName, topicName);

        assertThat(basicExternalKafkaClient.receiveMessagesTls(), is(MESSAGE_COUNT));

        String nonSuperuserName = userName + "-non-super-user";

        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(clusterName, nonSuperuserName)
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
            .build());

        LOGGER.info("Checking kafka super user:{} that is able to send messages to topic:{}", nonSuperuserName, topicName);

        basicExternalKafkaClient = basicExternalKafkaClient.toBuilder()
            .withKafkaUsername(nonSuperuserName)
            .build();

        assertThat(basicExternalKafkaClient.sendMessagesTls(), is(MESSAGE_COUNT));

        LOGGER.info("Checking kafka super user:{} that is not able to read messages to topic:{} because of defined" +
                " ACLs on only write operation", nonSuperuserName, topicName);

        BasicExternalKafkaClient newBasicExternalKafkaClient = basicExternalKafkaClient.toBuilder()
            .withConsumerGroupName(ClientUtils.generateRandomConsumerGroup())
            .build();

        assertThrows(WaitException.class, () -> newBasicExternalKafkaClient.receiveMessagesTls(Constants.GLOBAL_CLIENTS_EXCEPT_ERROR_TIMEOUT));
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(INTERNAL_CLIENTS_USED)
    void testCaRenewalBreakInMiddle(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 3)
            .editSpec()
                .withNewClusterCa()
                    .withRenewalDays(1)
                    .withValidityDays(3)
                .endClusterCa()
            .endSpec()
            .build());

        KafkaUser user = KafkaUserTemplates.tlsUser(clusterName, userName).build();

        resourceManager.createResource(extensionContext, user);
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, user).build());

        String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(defaultKafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withUsingPodName(defaultKafkaClientsPodName)
            .build();

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(),
            internalKafkaClient.receiveMessagesTls()
        );

        Map<String, String> zkPods = StatefulSetUtils.ssSnapshot(KafkaResources.zookeeperStatefulSetName(clusterName));
        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(clusterName));
        Map<String, String> eoPods = DeploymentUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(clusterName));

        InputStream secretInputStream = getClass().getClassLoader().getResourceAsStream("security-st-certs/expired-cluster-ca.crt");
        String clusterCaCert = TestUtils.readResource(secretInputStream);
        SecretUtils.createSecret(clusterCaCertificateSecretName(clusterName), "ca.crt", new String(Base64.getEncoder().encode(clusterCaCert.getBytes()), StandardCharsets.US_ASCII));

        KafkaResource.replaceKafkaResource(clusterName, k -> {
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
                List<Pod> pendingPods = kubeClient().listPodsByPrefixInName(KafkaResources.zookeeperStatefulSetName(clusterName))
                    .stream().filter(pod -> pod.getStatus().getPhase().equals("Pending")).collect(Collectors.toList());
                if (pendingPods.isEmpty()) {
                    LOGGER.info("No pods of {} are in desired state", KafkaResources.zookeeperStatefulSetName(clusterName));
                    return false;
                } else {
                    LOGGER.info("Pod in 'Pending' state: {}", pendingPods.get(0).getMetadata().getName());
                    return true;
                }
            }
        );

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withConsumerGroupName(ClientUtils.generateRandomConsumerGroup())
            .build();

        int received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(MESSAGE_COUNT));

        KafkaResource.replaceKafkaResource(clusterName, k -> {
            k.getSpec()
                .getZookeeper()
                .setResources(new ResourceRequirementsBuilder()
                    .addToRequests("cpu", new Quantity("200m"))
                    .build());
        });

        // Wait until the certificates have been replaced
        SecretUtils.waitForCertToChange(clusterCaCert, KafkaResources.clusterCaCertificateSecretName(clusterName));
        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.zookeeperStatefulSetName(clusterName), 3, zkPods);
        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(clusterName), 3, kafkaPods);
        DeploymentUtils.waitTillDepHasRolled(KafkaResources.entityOperatorDeploymentName(clusterName), 1, eoPods);

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withConsumerGroupName(ClientUtils.generateRandomConsumerGroup())
            .build();

        LOGGER.info("Checking produced and consumed messages to pod:{}", internalKafkaClient.getPodName());
        received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(MESSAGE_COUNT));

        // Try to send and receive messages with new certificates
        topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withConsumerGroupName(ClientUtils.generateRandomConsumerGroup())
            .withTopicName(topicName)
            .build();

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(),
            internalKafkaClient.receiveMessagesTls()
        );
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testKafkaAndKafkaConnectTlsVersion(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());
        Map<String, Object> configWithNewestVersionOfTls = new HashMap<>();

        final String tlsVersion12 = "TLSv1.2";
        final String tlsVersion1 = "TLSv1";

        configWithNewestVersionOfTls.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, tlsVersion12);
        configWithNewestVersionOfTls.put(SslConfigs.SSL_PROTOCOL_CONFIG, SslConfigs.DEFAULT_SSL_PROTOCOL);

        LOGGER.info("Deploying Kafka cluster with the support {} TLS",  tlsVersion12);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withConfig(configWithNewestVersionOfTls)
                .endKafka()
            .endSpec()
            .build());

        Map<String, Object> configsFromKafkaCustomResource = KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(clusterName).get().getSpec().getKafka().getConfig();

        LOGGER.info("Verifying that Kafka cluster has the accepted configuration:\n" +
                        "{} -> {}\n" +
                        "{} -> {}",
                SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG,
                configsFromKafkaCustomResource.get(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG),
                SslConfigs.SSL_PROTOCOL_CONFIG,
                configsFromKafkaCustomResource.get(SslConfigs.SSL_PROTOCOL_CONFIG));

        assertThat(configsFromKafkaCustomResource.get(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG), is(tlsVersion12));
        assertThat(configsFromKafkaCustomResource.get(SslConfigs.SSL_PROTOCOL_CONFIG), is(SslConfigs.DEFAULT_SSL_PROTOCOL));

        Map<String, Object> configWithLowestVersionOfTls = new HashMap<>();

        configWithLowestVersionOfTls.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, tlsVersion1);
        configWithLowestVersionOfTls.put(SslConfigs.SSL_PROTOCOL_CONFIG, tlsVersion1);

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(kafkaClientsName).build());
        resourceManager.createResource(extensionContext, false, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, clusterName, 1)
            .editSpec()
                .withConfig(configWithLowestVersionOfTls)
            .endSpec()
            .build());

        LOGGER.info("Verifying that Kafka Connect status is NotReady because of different TLS version");

        KafkaConnectUtils.waitForConnectStatus(clusterName, NotReady);

        LOGGER.info("Replacing Kafka Connect config to the newest(TLSv1.2) one same as the Kafka broker has.");

        KafkaConnectResource.replaceKafkaConnectResource(clusterName, kafkaConnect -> kafkaConnect.getSpec().setConfig(configWithNewestVersionOfTls));

        LOGGER.info("Verifying that Kafka Connect has the accepted configuration:\n {} -> {}\n {} -> {}",
                SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG,
                tlsVersion12,
                SslConfigs.SSL_PROTOCOL_CONFIG,
                SslConfigs.DEFAULT_SSL_PROTOCOL);

        KafkaConnectUtils.waitForKafkaConnectConfigChange(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, tlsVersion12, NAMESPACE, clusterName);
        KafkaConnectUtils.waitForKafkaConnectConfigChange(SslConfigs.SSL_PROTOCOL_CONFIG, SslConfigs.DEFAULT_SSL_PROTOCOL, NAMESPACE, clusterName);

        LOGGER.info("Verifying that Kafka Connect is stable");

        PodUtils.verifyThatRunningPodsAreStable(KafkaConnectResources.deploymentName(clusterName));

        LOGGER.info("Verifying that Kafka Connect status is Ready because of same TLS version");

        KafkaConnectUtils.waitForConnectReady(clusterName);

        KafkaConnectResource.kafkaConnectClient().inNamespace(NAMESPACE).withName(clusterName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
        DeploymentUtils.waitForDeploymentDeletion(KafkaConnectResources.deploymentName(clusterName));
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testKafkaAndKafkaConnectCipherSuites(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());
        Map<String, Object> configWithCipherSuitesSha384 = new HashMap<>();

        final String cipherSuitesSha384 = "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384";
        final String cipherSuitesSha256 = "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256";

        configWithCipherSuitesSha384.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, cipherSuitesSha384);

        LOGGER.info("Deploying Kafka cluster with the support {} cipher algorithms",  cipherSuitesSha384);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withConfig(configWithCipherSuitesSha384)
                .endKafka()
            .endSpec()
            .build());

        Map<String, Object> configsFromKafkaCustomResource = KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(clusterName).get().getSpec().getKafka().getConfig();

        LOGGER.info("Verifying that Kafka Connect has the accepted configuration:\n {} -> {}",
                SslConfigs.SSL_CIPHER_SUITES_CONFIG, configsFromKafkaCustomResource.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG));

        assertThat(configsFromKafkaCustomResource.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG), is(cipherSuitesSha384));

        Map<String, Object> configWithCipherSuitesSha256 = new HashMap<>();

        configWithCipherSuitesSha256.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, cipherSuitesSha256);

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(kafkaClientsName).build());

        resourceManager.createResource(extensionContext, false, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, clusterName, 1)
            .editSpec()
                .withConfig(configWithCipherSuitesSha256)
            .endSpec()
            .build());

        LOGGER.info("Verifying that Kafka Connect status is NotReady because of different cipher suites complexity of algorithm");

        KafkaConnectUtils.waitForConnectNotReady(clusterName);

        LOGGER.info("Replacing Kafka Connect config to the cipher suites same as the Kafka broker has.");

        KafkaConnectResource.replaceKafkaConnectResource(clusterName, kafkaConnect -> kafkaConnect.getSpec().setConfig(configWithCipherSuitesSha384));

        LOGGER.info("Verifying that Kafka Connect has the accepted configuration:\n {} -> {}",
                SslConfigs.SSL_CIPHER_SUITES_CONFIG, configsFromKafkaCustomResource.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG));

        KafkaConnectUtils.waitForKafkaConnectConfigChange(SslConfigs.SSL_CIPHER_SUITES_CONFIG, cipherSuitesSha384, NAMESPACE, clusterName);

        LOGGER.info("Verifying that Kafka Connect is stable");

        PodUtils.verifyThatRunningPodsAreStable(KafkaConnectResources.deploymentName(clusterName));

        LOGGER.info("Verifying that Kafka Connect status is Ready because of the same cipher suites complexity of algorithm");

        KafkaConnectUtils.waitForConnectReady(clusterName);

        KafkaConnectResource.kafkaConnectClient().inNamespace(NAMESPACE).withName(clusterName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
        DeploymentUtils.waitForDeploymentDeletion(KafkaConnectResources.deploymentName(clusterName));
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testOwnerReferenceOfCASecrets(ExtensionContext extensionContext) {
        /* Different name for Kafka cluster to make the test quicker -> KafkaRoller is waiting for pods of "my-cluster" to become ready
         for 5 minutes -> this will prevent the waiting. */
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String secondClusterName = "my-second-cluster-" + clusterName;

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editOrNewSpec()
                .withNewClusterCa()
                    .withGenerateSecretOwnerReference(false)
                .endClusterCa()
                .withNewClientsCa()
                    .withGenerateSecretOwnerReference(false)
                .endClientsCa()
            .endSpec()
            .build());

        LOGGER.info("Listing all cluster CAs for {}", clusterName);
        List<Secret> caSecrets = kubeClient().listSecrets().stream()
            .filter(secret -> secret.getMetadata().getName().contains(KafkaResources.clusterCaKeySecretName(clusterName)) || secret.getMetadata().getName().contains(KafkaResources.clientsCaKeySecretName(clusterName))).collect(Collectors.toList());

        LOGGER.info("Deleting Kafka:{}", clusterName);
        KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(clusterName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
        KafkaUtils.waitForKafkaDeletion(clusterName);

        LOGGER.info("Checking actual secrets after Kafka deletion");
        caSecrets.forEach(caSecret -> {
            String secretName = caSecret.getMetadata().getName();
            LOGGER.info("Checking that {} secret is still present", secretName);
            assertNotNull(kubeClient().getSecret(secretName));

            LOGGER.info("Deleting secret: {}", secretName);
            kubeClient().deleteSecret(secretName);
        });

        LOGGER.info("Deploying Kafka with generateSecretOwnerReference set to true");
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(secondClusterName, 3)
            .editOrNewSpec()
                .editOrNewClusterCa()
                    .withGenerateSecretOwnerReference(true)
                .endClusterCa()
                .editOrNewClientsCa()
                    .withGenerateSecretOwnerReference(true)
                .endClientsCa()
            .endSpec()
            .build());

        caSecrets = kubeClient().listSecrets().stream()
            .filter(secret -> secret.getMetadata().getName().contains(KafkaResources.clusterCaKeySecretName(secondClusterName)) || secret.getMetadata().getName().contains(KafkaResources.clientsCaKeySecretName(secondClusterName))).collect(Collectors.toList());

        LOGGER.info("Deleting Kafka:{}", secondClusterName);
        KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(secondClusterName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
        KafkaUtils.waitForKafkaDeletion(secondClusterName);

        LOGGER.info("Checking actual secrets after Kafka deletion");
        caSecrets.forEach(caSecret -> {
            String secretName = caSecret.getMetadata().getName();
            LOGGER.info("Checking that {} secret is deleted", secretName);
            assertNull(kubeClient().getSecret(secretName));
        });
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testClusterCACertRenew(ExtensionContext extensionContext) {
        checkClusterCACertRenew(extensionContext, false);
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testCustomClusterCACertRenew(ExtensionContext extensionContext) {
        checkClusterCACertRenew(extensionContext, true);
    }

    void checkClusterCACertRenew(ExtensionContext extensionContext, boolean customCA) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        if (customCA) {
            generateAndDeployCustomStrimziCA(clusterName);
            checkCustomCAsCorrectness(clusterName);
            resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
                .editOrNewSpec()
                    .withNewClusterCa()
                        .withRenewalDays(15)
                        .withValidityDays(20)
                        .withGenerateCertificateAuthority(false)
                    .endClusterCa()
                .endSpec()
                .build());
        } else {
            resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
                .editOrNewSpec()
                    .withNewClusterCa()
                        .withRenewalDays(15)
                        .withValidityDays(20)
                    .endClusterCa()
                .endSpec()
                .build());
        }

        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(clusterName));

        Secret clusterCASecret = kubeClient().getSecret(KafkaResources.clusterCaCertificateSecretName(clusterName));
        X509Certificate cacert = SecretUtils.getCertificateFromSecret(clusterCASecret, "ca.crt");
        Date initialCertStartTime = cacert.getNotBefore();
        Date initialCertEndTime = cacert.getNotAfter();

        // Check Broker kafka certificate dates
        Secret brokerCertCreationSecret = kubeClient().getSecret(clusterName + "-kafka-brokers");
        X509Certificate kafkaBrokerCert = SecretUtils.getCertificateFromSecret(brokerCertCreationSecret, clusterName + "-kafka-0.crt");
        Date initialKafkaBrokerCertStartTime = kafkaBrokerCert.getNotBefore();
        Date initialKafkaBrokerCertEndTime = kafkaBrokerCert.getNotAfter();

        // Check Zookeeper certificate dates
        Secret zkCertCreationSecret = kubeClient().getSecret(clusterName + "-zookeeper-nodes");
        X509Certificate zkBrokerCert = SecretUtils.getCertificateFromSecret(zkCertCreationSecret, clusterName + "-zookeeper-0.crt");
        Date initialZkCertStartTime = zkBrokerCert.getNotBefore();
        Date initialZkCertEndTime = zkBrokerCert.getNotAfter();

        LOGGER.info("Change of kafka validity and renewal days - reconciliation should start.");
        CertificateAuthority newClusterCA = new CertificateAuthority();
        newClusterCA.setRenewalDays(150);
        newClusterCA.setValidityDays(200);
        if (customCA) {
            newClusterCA.setGenerateCertificateAuthority(false);
        }
        KafkaResource.replaceKafkaResource(clusterName, k -> k.getSpec().setClusterCa(newClusterCA));

        // Wait for reconciliation and verify certs have been updated
        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(clusterName), 3, kafkaPods);

        // Read renewed secret/certs again
        clusterCASecret = kubeClient().getSecret(KafkaResources.clusterCaCertificateSecretName(clusterName));
        cacert = SecretUtils.getCertificateFromSecret(clusterCASecret, "ca.crt");
        Date changedCertStartTime = cacert.getNotBefore();
        Date changedCertEndTime = cacert.getNotAfter();

        // Check renewed Broker kafka certificate dates
        brokerCertCreationSecret = kubeClient().getSecret(clusterName + "-kafka-brokers");
        kafkaBrokerCert = SecretUtils.getCertificateFromSecret(brokerCertCreationSecret, clusterName + "-kafka-0.crt");
        Date changedKafkaBrokerCertStartTime = kafkaBrokerCert.getNotBefore();
        Date changedKafkaBrokerCertEndTime = kafkaBrokerCert.getNotAfter();

        // Check renewed Zookeeper certificate dates
        zkCertCreationSecret = kubeClient().getSecret(clusterName + "-zookeeper-nodes");
        zkBrokerCert = SecretUtils.getCertificateFromSecret(zkCertCreationSecret, clusterName + "-zookeeper-0.crt");
        Date changedZkCertStartTime = zkBrokerCert.getNotBefore();
        Date changedZkCertEndTime = zkBrokerCert.getNotAfter();

        LOGGER.info("Initial ClusterCA cert dates: " + initialCertStartTime + " --> " + initialCertEndTime);
        LOGGER.info("Changed ClusterCA cert dates: " + changedCertStartTime + " --> " + changedCertEndTime);
        LOGGER.info("KafkaBroker cert creation dates: " + initialKafkaBrokerCertStartTime + " --> " + initialKafkaBrokerCertEndTime);
        LOGGER.info("KafkaBroker cert changed dates:  " + changedKafkaBrokerCertStartTime + " --> " + changedKafkaBrokerCertEndTime);
        LOGGER.info("Zookeeper cert creation dates: " + initialZkCertStartTime + " --> " + initialZkCertEndTime);
        LOGGER.info("Zookeeper cert changed dates:  " + changedZkCertStartTime + " --> " + changedZkCertEndTime);

        if (customCA) {
            assertThat("ClusterCA cert should not have changed.",
                    initialCertEndTime.compareTo(changedCertEndTime) == 0);
        } else {
            String msg = "Error: original cert-end date: '" + initialCertEndTime +
                    "' ends sooner than changed (prolonged) cert date '" + changedCertEndTime + "'!";
            assertThat(msg, initialCertEndTime.compareTo(changedCertEndTime) < 0);
        }
        assertThat("Broker certificates start dates have not been renewed.",
                initialKafkaBrokerCertStartTime.compareTo(changedKafkaBrokerCertStartTime) < 0);
        assertThat("Broker certificates end dates have not been renewed.",
                initialKafkaBrokerCertEndTime.compareTo(changedKafkaBrokerCertEndTime) < 0);
        assertThat("Zookeeper certificates start dates have not been renewed.",
                initialZkCertStartTime.compareTo(changedZkCertStartTime) < 0);
        assertThat("Zookeeper certificates end dates have not been renewed.",
                initialZkCertEndTime.compareTo(changedZkCertEndTime) < 0);
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testClientsCACertRenew(ExtensionContext extensionContext) {
        checkClientsCACertRenew(extensionContext, false);
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testCustomClientsCACertRenew(ExtensionContext extensionContext) {
        checkClientsCACertRenew(extensionContext, true);
    }

    void checkClientsCACertRenew(ExtensionContext extensionContext, boolean customCA) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        if (customCA) {
            generateAndDeployCustomStrimziCA(clusterName);
            checkCustomCAsCorrectness(clusterName);
            resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
                .editOrNewSpec()
                    .withNewClientsCa()
                        .withRenewalDays(15)
                        .withValidityDays(20)
                        .withGenerateCertificateAuthority(false)
                    .endClientsCa()
                .endSpec()
                .build());
        } else {
            resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
                .editOrNewSpec()
                    .withNewClientsCa()
                        .withRenewalDays(15)
                        .withValidityDays(20)
                    .endClientsCa()
                .endSpec()
                .build());
        }

        String username = "strimzi-tls-user-" + new Random().nextInt(Integer.MAX_VALUE);
        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(clusterName, username).build());
        Map<String, String> entityPods = DeploymentUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(clusterName));

        // Check initial clientsCA validity days
        Secret clientsCASecret = kubeClient().getSecret(KafkaResources.clientsCaCertificateSecretName(clusterName));
        X509Certificate cacert = SecretUtils.getCertificateFromSecret(clientsCASecret, "ca.crt");
        Date initialCertStartTime = cacert.getNotBefore();
        Date initialCertEndTime = cacert.getNotAfter();

        // Check initial kafkauser validity days
        X509Certificate userCert = SecretUtils.getCertificateFromSecret(kubeClient().getSecret(username), "user.crt");
        Date initialKafkaUserCertStartTime = userCert.getNotBefore();
        Date initialKafkaUserCertEndTime = userCert.getNotAfter();

        LOGGER.info("Change of kafka validity and renewal days - reconciliation should start.");
        CertificateAuthority newClientsCA = new CertificateAuthority();
        newClientsCA.setRenewalDays(150);
        newClientsCA.setValidityDays(200);
        if (customCA) {
            newClientsCA.setGenerateCertificateAuthority(false);
        }
        KafkaResource.replaceKafkaResource(clusterName, k -> k.getSpec().setClientsCa(newClientsCA));

        // Wait for reconciliation and verify certs have been updated
        DeploymentUtils.waitTillDepHasRolled(KafkaResources.entityOperatorDeploymentName(clusterName), 1, entityPods);

        // Read renewed secret/certs again
        clientsCASecret = kubeClient().getSecret(KafkaResources.clientsCaCertificateSecretName(clusterName));
        cacert = SecretUtils.getCertificateFromSecret(clientsCASecret, "ca.crt");
        Date changedCertStartTime = cacert.getNotBefore();
        Date changedCertEndTime = cacert.getNotAfter();

        userCert = SecretUtils.getCertificateFromSecret(kubeClient().getSecret(username), "user.crt");
        Date changedKafkaUserCertStartTime = userCert.getNotBefore();
        Date changedKafkaUserCertEndTime = userCert.getNotAfter();

        LOGGER.info("Initial ClientsCA cert dates: " + initialCertStartTime + " --> " + initialCertEndTime);
        LOGGER.info("Changed ClientsCA cert dates: " + changedCertStartTime + " --> " + changedCertEndTime);
        LOGGER.info("Initial userCert dates: " + initialKafkaUserCertStartTime + " --> " + initialKafkaUserCertEndTime);
        LOGGER.info("Changed userCert dates: " + changedKafkaUserCertStartTime + " --> " + changedKafkaUserCertEndTime);

        if (customCA) {
            assertThat("ClientsCA cert should not have changed.",
                    initialCertEndTime.compareTo(changedCertEndTime) == 0);
        } else {
            String msg = "Error: original cert-end date: '" + initialCertEndTime +
                    "' ends sooner than changed (prolonged) cert date '" + changedCertEndTime + "'";
            assertThat(msg, initialCertEndTime.compareTo(changedCertEndTime) < 0);
        }
        assertThat("UserCert start date has been renewed",
                initialKafkaUserCertStartTime.compareTo(changedKafkaUserCertStartTime) < 0);
        assertThat("UserCert end date has been renewed",
                initialKafkaUserCertEndTime.compareTo(changedKafkaUserCertEndTime) < 0);
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testCustomClusterCAClientsCA(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());

        generateAndDeployCustomStrimziCA(clusterName);
        checkCustomCAsCorrectness(clusterName);

        LOGGER.info(" Deploy kafka with new certs/secrets.");
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3, 3)
            .editSpec()
                .withNewClusterCa()
                    .withGenerateCertificateAuthority(false)
                .endClusterCa()
                .withNewClientsCa()
                    .withGenerateCertificateAuthority(false)
                .endClientsCa()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withType(KafkaListenerType.INTERNAL)
                            .withName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
                            .withPort(9092)
                            .withTls(false)
                        .endGenericKafkaListener()
                        .addNewGenericKafkaListener()
                            .withType(KafkaListenerType.INTERNAL)
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withTls(true)
                            .withNewKafkaListenerAuthenticationTlsAuth()
                            .endKafkaListenerAuthenticationTlsAuth()
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        LOGGER.info("Check Kafka(s) and Zookeeper(s) certificates.");
        X509Certificate kafkaCert = SecretUtils.getCertificateFromSecret(kubeClient().getSecret(clusterName + "-kafka-brokers"), clusterName + "-kafka-0.crt");
        assertThat("KafkaCert does not have expected test Issuer: " + kafkaCert.getIssuerDN(),
                SystemTestCertManager.containsAllDN(kafkaCert.getIssuerX500Principal().getName(), STRIMZI_TEST_CLUSTER_CA));

        X509Certificate zookeeperCert = SecretUtils.getCertificateFromSecret(kubeClient().getSecret(clusterName + "-zookeeper-nodes"), clusterName + "-zookeeper-0.crt");
        assertThat("ZookeeperCert does not have expected test Subject: " + zookeeperCert.getIssuerDN(),
                SystemTestCertManager.containsAllDN(zookeeperCert.getIssuerX500Principal().getName(), STRIMZI_TEST_CLUSTER_CA));

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());

        LOGGER.info("Check KafkaUser certificate.");
        KafkaUser user = KafkaUserTemplates.tlsUser(clusterName, userName).build();
        resourceManager.createResource(extensionContext, user);
        X509Certificate userCert = SecretUtils.getCertificateFromSecret(kubeClient().getSecret(userName), "user.crt");
        assertThat("Generated ClientsCA does not have expected test Subject: " + userCert.getIssuerDN(),
                SystemTestCertManager.containsAllDN(userCert.getIssuerX500Principal().getName(), STRIMZI_TEST_CLIENTS_CA));

        LOGGER.info("Send and receive messages over TLS.");
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, user).build());
        final String kafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
                .withUsingPodName(kafkaClientsPodName)
                .withTopicName(topicName)
                .withNamespaceName(NAMESPACE)
                .withClusterName(clusterName)
                .withKafkaUsername(userName)
                .withMessageCount(MESSAGE_COUNT)
                .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
                .build();

        LOGGER.info("Check for certificates used within kafka pod internal clients (producer/consumer)");
        List<VolumeMount> volumeMounts = ResourceManager.kubeClient().listPodsByPrefixInName(clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getSpec().getContainers().get(0).getVolumeMounts();
        for (VolumeMount vm : volumeMounts) {
            if (vm.getMountPath().contains("user-secret-" + internalKafkaClient.getKafkaUsername())) {
                assertThat("UserCert Issuer DN in clients pod is incorrect!", checkMountVolumeSecret(kafkaClientsPodName,
                        vm, "issuer", STRIMZI_INTERMEDIATE_CA));
                assertThat("UserCert Subject DN in clients pod is incorrect!", checkMountVolumeSecret(kafkaClientsPodName,
                        vm, "subject", STRIMZI_TEST_CLIENTS_CA));

            } else if (vm.getMountPath().contains("cluster-ca-" + internalKafkaClient.getKafkaUsername())) {
                assertThat("ClusterCA Issuer DN in clients pod is incorrect!", checkMountVolumeSecret(kafkaClientsPodName,
                        vm, "issuer", STRIMZI_INTERMEDIATE_CA));
                assertThat("ClusterCA Subject DN in clients pod is incorrect!", checkMountVolumeSecret(kafkaClientsPodName,
                        vm, "subject", STRIMZI_TEST_CLUSTER_CA));
            }
        }

        LOGGER.info("Checking produced and consumed messages via TLS to pod:{}", kafkaClientsPodName);
        internalKafkaClient.checkProducedAndConsumedMessages(
                internalKafkaClient.sendMessagesTls(),
                internalKafkaClient.receiveMessagesTls()
        );
    }

    void checkCustomCAsCorrectness(String clusterName) {
        LOGGER.info("Check ClusterCA and ClientsCA certificates.");
        X509Certificate clientsCert = SecretUtils.getCertificateFromSecret(kubeClient().getSecret(KafkaResources.clientsCaCertificateSecretName(clusterName)), "ca.crt");
        X509Certificate clusterCert = SecretUtils.getCertificateFromSecret(kubeClient().getSecret(KafkaResources.clusterCaCertificateSecretName(clusterName)), "ca.crt");

        assertThat("Generated ClientsCA does not have expected Issuer: " + clientsCert.getIssuerDN(),
                SystemTestCertManager.containsAllDN(clientsCert.getIssuerX500Principal().getName(), STRIMZI_INTERMEDIATE_CA));
        assertThat("Generated ClientsCA does not have expected Subject: " + clientsCert.getSubjectDN(),
                SystemTestCertManager.containsAllDN(clientsCert.getSubjectX500Principal().getName(), STRIMZI_TEST_CLIENTS_CA));

        assertThat("Generated ClusterCA does not have expected Issuer: " + clusterCert.getIssuerDN(),
                SystemTestCertManager.containsAllDN(clusterCert.getIssuerX500Principal().getName(), STRIMZI_INTERMEDIATE_CA));
        assertThat("Generated ClusterCA does not have expected Subject: " + clusterCert.getSubjectDN(),
                SystemTestCertManager.containsAllDN(clusterCert.getSubjectX500Principal().getName(), STRIMZI_TEST_CLUSTER_CA));
    }

    boolean checkMountVolumeSecret(String podName, VolumeMount volumeMount, String principalDNType, String expectedPrincipal) {
        String dn = ResourceManager.cmdKubeClient().execInPod(podName, "/bin/bash", "-c",
                "openssl x509 -in " + volumeMount.getMountPath() + "/ca.crt -noout -" + principalDNType).out().strip();
        String certOutIssuer = dn.substring(principalDNType.length() + 3).replace("/", ",");
        return SystemTestCertManager.containsAllDN(certOutIssuer, expectedPrincipal);
    }

    void generateAndDeployCustomStrimziCA(String clusterName) {
        LOGGER.info("Generating custom RootCA, IntermediateCA, and ClusterCA, ClientsCA for Strimzi and PEM bundles.");
        SystemTestCertAndKey strimziRootCA = SystemTestCertManager.generateRootCaCertAndKey();
        SystemTestCertAndKey intermediateCA = SystemTestCertManager.generateIntermediateCaCertAndKey(strimziRootCA);
        SystemTestCertAndKey stClusterCA = SystemTestCertManager.generateStrimziCaCertAndKey(intermediateCA, STRIMZI_TEST_CLUSTER_CA);
        SystemTestCertAndKey stClientsCA = SystemTestCertManager.generateStrimziCaCertAndKey(intermediateCA, STRIMZI_TEST_CLIENTS_CA);

        // Create PEM bundles (strimzi root CA, intermediate CA, cluster|clients CA cert+key) for ClusterCA and ClientsCA
        CertAndKeyFiles clusterBundle = SystemTestCertManager.exportToPemFiles(stClusterCA, intermediateCA, strimziRootCA);
        CertAndKeyFiles clientsBundle = SystemTestCertManager.exportToPemFiles(stClientsCA, intermediateCA, strimziRootCA);

        Map<String, String> secretLabels = new HashMap<>();
        secretLabels.put(Labels.STRIMZI_CLUSTER_LABEL, clusterName);
        secretLabels.put(Labels.STRIMZI_KIND_LABEL, "Kafka");

        try {
            // Deploy ClusterCA secret
            LOGGER.info("Deploy all certificates and keys as secrets.");
            SecretUtils.deleteSecretWithWait(KafkaResources.clusterCaCertificateSecretName(clusterName), NAMESPACE);
            SecretUtils.createCustomSecret(KafkaResources.clusterCaCertificateSecretName(clusterName), clusterName, NAMESPACE, clusterBundle);


            SecretUtils.deleteSecretWithWait(KafkaResources.clusterCaKeySecretName(clusterName), NAMESPACE);
            File strimziKeyPKCS8 = convertPrivateKeyToPKCS8File(stClusterCA.getPrivateKey());
            SecretUtils.createSecretFromFile(strimziKeyPKCS8.getAbsolutePath(), "ca.key", KafkaResources.clusterCaKeySecretName(clusterName), NAMESPACE, secretLabels);

            // ClientsCA secret part
            SecretUtils.deleteSecretWithWait(KafkaResources.clientsCaCertificateSecretName(clusterName), NAMESPACE);
            SecretUtils.createCustomSecret(KafkaResources.clientsCaCertificateSecretName(clusterName), clusterName, NAMESPACE, clientsBundle);

            SecretUtils.deleteSecretWithWait(KafkaResources.clientsCaKeySecretName(clusterName), NAMESPACE);
            File clientsKeyPKCS8 = convertPrivateKeyToPKCS8File(stClientsCA.getPrivateKey());
            SecretUtils.createSecretFromFile(clientsKeyPKCS8.getAbsolutePath(), "ca.key", KafkaResources.clientsCaKeySecretName(clusterName), NAMESPACE, secretLabels);

        } catch (NoSuchAlgorithmException | InvalidKeySpecException | IOException e) {
            e.printStackTrace();
        }
    }


    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        installClusterOperator(extensionContext, NAMESPACE, Constants.CO_OPERATION_TIMEOUT_DEFAULT);
    }
}
