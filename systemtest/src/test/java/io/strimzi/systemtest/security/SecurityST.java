/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.common.CertificateAuthority;
import io.strimzi.api.kafka.model.common.CertificateAuthorityBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlResources;
import io.strimzi.api.kafka.model.kafka.exporter.KafkaExporterResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.mirrormaker.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.user.acl.AclOperation;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.kafkaclients.externalClients.ExternalKafkaClient;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMakerResource;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMakerTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMakerUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.test.TestUtils;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.io.InputStream;
import java.security.cert.X509Certificate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.strimzi.api.kafka.model.kafka.KafkaResources.clientsCaCertificateSecretName;
import static io.strimzi.api.kafka.model.kafka.KafkaResources.clientsCaKeySecretName;
import static io.strimzi.api.kafka.model.kafka.KafkaResources.clusterCaCertificateSecretName;
import static io.strimzi.api.kafka.model.kafka.KafkaResources.clusterCaKeySecretName;
import static io.strimzi.systemtest.TestConstants.ACCEPTANCE;
import static io.strimzi.systemtest.TestConstants.CONNECT;
import static io.strimzi.systemtest.TestConstants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.TestConstants.CRUISE_CONTROL;
import static io.strimzi.systemtest.TestConstants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.TestConstants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.TestConstants.MIRROR_MAKER;
import static io.strimzi.systemtest.TestConstants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static io.strimzi.systemtest.TestConstants.ROLLING_UPDATE;
import static io.strimzi.systemtest.TestConstants.SANITY;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(REGRESSION)
class SecurityST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(SecurityST.class);
    private static final String OPENSSL_RETURN_CODE = "Verify return code: 0 (ok)";

    @ParallelNamespaceTest
    void testCertificates() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        LOGGER.info("Running testCertificates {}", testStorage.getClusterName());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3).build());

        String brokerPodName = kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector()).get(0).getMetadata().getName();

        LOGGER.info("Check Kafka bootstrap certificate");
        String outputCertificate = SystemTestCertManager.generateOpenSslCommandByComponent(testStorage.getNamespaceName(), KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()), KafkaResources.bootstrapServiceName(testStorage.getClusterName()),
            brokerPodName, "kafka", false);
        LOGGER.info("OPENSSL OUTPUT: \n\n{}\n\n", outputCertificate);
        verifyCerts(testStorage.getClusterName(), outputCertificate, "kafka");

        if (!Environment.isKRaftModeEnabled()) {
            LOGGER.info("Check ZooKeeper client certificate");
            outputCertificate = SystemTestCertManager.generateOpenSslCommandByComponent(testStorage.getNamespaceName(), KafkaResources.zookeeperServiceName(testStorage.getClusterName()) + ":2181", KafkaResources.zookeeperServiceName(testStorage.getClusterName()),
                brokerPodName, "kafka");
            verifyCerts(testStorage.getClusterName(), outputCertificate, "zookeeper");
        }

        List<String> kafkaPorts = new ArrayList<>(Arrays.asList("9091", "9093"));
        List<String> zkPorts = new ArrayList<>(Arrays.asList("2181", "3888"));
        List<String> brokerPods = kubeClient().listPodNames(testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        brokerPods.forEach(brokerPod -> {
            String output;

            LOGGER.info("Checking certificates for {}", brokerPod);
            for (String kafkaPort : kafkaPorts) {
                LOGGER.info("Check Kafka certificate for port {}", kafkaPort);
                output = SystemTestCertManager.generateOpenSslCommandByComponentUsingSvcHostname(testStorage.getNamespaceName(),
                    brokerPod,
                    KafkaResources.brokersServiceName(testStorage.getClusterName()), kafkaPort, "kafka");
                verifyCerts(testStorage.getClusterName(), output, "kafka");
            }
        });

        IntStream.rangeClosed(0, 2).forEach(podId -> {
            String output;

            if (!Environment.isKRaftModeEnabled()) {
                for (String zkPort : zkPorts) {
                    LOGGER.info("Check ZooKeeper certificate for port {}", zkPort);
                    output = SystemTestCertManager.generateOpenSslCommandByComponentUsingSvcHostname(testStorage.getNamespaceName(), KafkaResources.zookeeperPodName(testStorage.getClusterName(), podId),
                            KafkaResources.zookeeperHeadlessServiceName(testStorage.getClusterName()), zkPort, "zookeeper");
                    verifyCerts(testStorage.getClusterName(), output, "zookeeper");
                }
            }
        });
    }

    // synchronized avoiding data-race (list of string is allocated on the heap), but has different reference on stack it's ok
    // but Strings parameters provided are not created in scope of this method
    synchronized private static void verifyCerts(String clusterName, String certificate, String component) {
        List<String> certificateChains = SystemTestCertManager.getCertificateChain(clusterName + "-" + component);

        assertThat(certificate, containsString(certificateChains.get(0)));
        assertThat(certificate, containsString(certificateChains.get(1)));
        assertThat(certificate, containsString(OPENSSL_RETURN_CODE));
    }

    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    @Tag(ROLLING_UPDATE)
    @Tag(CRUISE_CONTROL)
    @Tag("ClusterCaCerts")
    void testAutoRenewClusterCaCertsTriggeredByAnno() {
        autoRenewSomeCaCertsTriggeredByAnno(
                /* ZK node need new certs */
                true,
                /* brokers need new certs */
                true,
                /* eo needs new cert */
                true,
                true);
    }

    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    @Tag(ROLLING_UPDATE)
    @Tag(CRUISE_CONTROL)
    @Tag("ClientsCaCerts")
    void testAutoRenewClientsCaCertsTriggeredByAnno() {
        autoRenewSomeCaCertsTriggeredByAnno(
                /* no communication between clients and zk, so no need to roll */
                false,
                /* brokers need to trust client certs with new cert */
                true,
                /* eo needs to generate new client certs */
                false,
                false);
    }

    @ParallelNamespaceTest
    @Tag(SANITY)
    @Tag(ACCEPTANCE)
    @Tag(INTERNAL_CLIENTS_USED)
    @Tag(ROLLING_UPDATE)
    @Tag(CRUISE_CONTROL)
    @Tag("AllCaCerts")
    void testAutoRenewAllCaCertsTriggeredByAnno() {
        autoRenewSomeCaCertsTriggeredByAnno(
                true,
                true,
                true,
                true);
    }

    @SuppressWarnings({"checkstyle:MethodLength", "checkstyle:NPathComplexity"})
    void autoRenewSomeCaCertsTriggeredByAnno(
            boolean zkShouldRoll,
            boolean kafkaShouldRoll,
            boolean eoShouldRoll,
            boolean keAndCCShouldRoll) {

        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        createKafkaCluster(testStorage);

        List<String> secrets;

        // to make it parallel we need decision maker...
        if (ResourceManager.getTestContext().getTags().contains("ClusterCaCerts")) {
            secrets = Arrays.asList(clusterCaCertificateSecretName(testStorage.getClusterName()));
        } else if (ResourceManager.getTestContext().getTags().contains("ClientsCaCerts")) {
            secrets = Arrays.asList(clientsCaCertificateSecretName(testStorage.getClusterName()));
        } else {
            // AllCaKeys
            secrets = Arrays.asList(clusterCaCertificateSecretName(testStorage.getClusterName()),
                clientsCaCertificateSecretName(testStorage.getClusterName()));
        }

        resourceManager.createResourceWithWait(
            KafkaUserTemplates.tlsUser(testStorage).build(),
            KafkaTopicTemplates.topic(testStorage).build()
        );

        KafkaClients kafkaClients = ClientUtils.getInstantTlsClients(testStorage);
        resourceManager.createResourceWithWait(kafkaClients.producerTlsStrimzi(testStorage.getClusterName()), kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantClientSuccess(testStorage);

        // Get all pods, and their resource versions
        Map<String, String> controllerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());
        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        Map<String, String> eoPod = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), testStorage.getEoDeploymentName());
        Map<String, String> ccPod = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName()));
        Map<String, String> kePod = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), KafkaExporterResources.componentName(testStorage.getClusterName()));

        LOGGER.info("Triggering CA cert renewal by adding the annotation");
        Map<String, String> initialCaCerts = new HashMap<>();
        for (String secretName : secrets) {
            Secret secret = kubeClient().getSecret(testStorage.getNamespaceName(), secretName);
            String value = secret.getData().get("ca.crt");
            assertThat("ca.crt in " + secretName + " should not be null", value, is(notNullValue()));
            initialCaCerts.put(secretName, value);
            Secret annotated = new SecretBuilder(secret)
                .editMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_FORCE_RENEW, "true")
                .endMetadata()
                .build();
            LOGGER.info("Patching Secret: {} with {}", secretName, Annotations.ANNO_STRIMZI_IO_FORCE_RENEW);
            kubeClient().patchSecret(testStorage.getNamespaceName(), secretName, annotated);
        }

        if (!Environment.isKRaftModeEnabled()) {
            if (zkShouldRoll) {
                LOGGER.info("Waiting for ZK rolling restart");
                RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getControllerSelector(), 3, controllerPods);
            }
        }
        if (kafkaShouldRoll) {
            LOGGER.info("Waiting for Kafka rolling restart");
            RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);
        }
        if (eoShouldRoll) {
            LOGGER.info("Waiting for EO rolling restart");
            eoPod = DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), testStorage.getEoDeploymentName(), 1, eoPod);
        }
        if (keAndCCShouldRoll) {
            LOGGER.info("Waiting for CC and KE rolling restart");
            kePod = DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), KafkaExporterResources.componentName(testStorage.getClusterName()), 1, kePod);
            ccPod = DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName()), 1, ccPod);
        }

        LOGGER.info("Ensuring the certificates have been replaced");
        for (String secretName : secrets) {
            Secret secret = kubeClient().getSecret(testStorage.getNamespaceName(), secretName);
            assertThat("Secret " + secretName + " should exist", secret, is(notNullValue()));
            assertThat("CA cert in " + secretName + " should have non-null 'data'", is(notNullValue()));
            String value = secret.getData().get("ca.crt");
            assertThat("CA cert in " + secretName + " should have changed",
                value, is(not(initialCaCerts.get(secretName))));
        }

        kafkaClients.generateNewConsumerGroup();
        resourceManager.createResourceWithWait(kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        // Check a new client (signed by new client key) can consume
        String bobUserName = "bob-" + testStorage.getUsername();

        resourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), testStorage.getClusterName(), bobUserName).build());

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .withUsername(bobUserName)
            .build();

        resourceManager.createResourceWithWait(kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        if (!Environment.isKRaftModeEnabled()) {
            if (!zkShouldRoll) {
                assertThat("ZK Pods should not roll, but did.", PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector()), is(controllerPods));
            }
        }
        if (!kafkaShouldRoll) {
            assertThat("Kafka Pods should not roll, but did.", PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector()), is(brokerPods));
        }
        if (!eoShouldRoll) {
            assertThat("EO Pod should not roll, but did.", DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), testStorage.getEoDeploymentName()), is(eoPod));
        }
        if (!keAndCCShouldRoll) {
            assertThat("CC Pod should not roll, but did.", DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName())), is(ccPod));
            assertThat("KE Pod should not roll, but did.", DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), KafkaExporterResources.componentName(testStorage.getClusterName())), is(kePod));
        }
    }

    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    @Tag(ROLLING_UPDATE)
    @Tag(CRUISE_CONTROL)
    @Tag("ClusterCaKeys")
    void testAutoReplaceClusterCaKeysTriggeredByAnno() {
        autoReplaceSomeKeysTriggeredByAnno(
                3, // additional third rolling due to the removal of the older cluster CA certificate
                true,
                true,
                true,
                true);
    }

    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    @Tag(ROLLING_UPDATE)
    @Tag(CRUISE_CONTROL)
    @Tag("ClientsCaKeys")
    void testAutoReplaceClientsCaKeysTriggeredByAnno() {
        autoReplaceSomeKeysTriggeredByAnno(
                2,
                false,
                true,
                false,
                false);
    }

    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    @Tag(ROLLING_UPDATE)
    @Tag(CRUISE_CONTROL)
    @Tag("AllCaKeys")
    void testAutoReplaceAllCaKeysTriggeredByAnno() {
        autoReplaceSomeKeysTriggeredByAnno(
                3, // additional third rolling due to the removal of the older cluster CA certificate
                true,
                true,
                true,
                true);
    }

    @SuppressWarnings({"checkstyle:MethodLength", "checkstyle:NPathComplexity", "checkstyle:CyclomaticComplexity"})
    void autoReplaceSomeKeysTriggeredByAnno(int expectedRolls,
                                            boolean zkShouldRoll,
                                            boolean kafkaShouldRoll,
                                            boolean eoShouldRoll,
                                            boolean keAndCCShouldRoll) {

        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        List<String> secrets = null;

        // to make it parallel we need decision maker...
        if (ResourceManager.getTestContext().getTags().contains("ClusterCaKeys")) {
            secrets = Arrays.asList(clusterCaKeySecretName(testStorage.getClusterName()));
        } else if (ResourceManager.getTestContext().getTags().contains("ClientsCaKeys")) {
            secrets = Arrays.asList(clientsCaKeySecretName(testStorage.getClusterName()));
        } else {
            // AllCaKeys
            secrets = Arrays.asList(clusterCaKeySecretName(testStorage.getClusterName()),
                clientsCaKeySecretName(testStorage.getClusterName()));
        }

        createKafkaCluster(testStorage);

        resourceManager.createResourceWithWait(
            KafkaUserTemplates.tlsUser(testStorage).build(),
            KafkaTopicTemplates.topic(testStorage).build()
        );

        KafkaClients kafkaClients = ClientUtils.getInstantTlsClients(testStorage);
        resourceManager.createResourceWithWait(kafkaClients.producerTlsStrimzi(testStorage.getClusterName()), kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantClientSuccess(testStorage);

        // Get all pods, and their resource versions
        Map<String, String> controllerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());
        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        Map<String, String> eoPod = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), testStorage.getEoDeploymentName());
        Map<String, String> ccPod = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName()));
        Map<String, String> kePod = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), KafkaExporterResources.componentName(testStorage.getClusterName()));

        LOGGER.info("Triggering CA cert renewal by adding the annotation");
        Map<String, String> initialCaKeys = new HashMap<>();
        for (String secretName : secrets) {
            Secret secret = kubeClient().getSecret(testStorage.getNamespaceName(), secretName);
            String value = secret.getData().get("ca.key");
            assertThat("ca.key in " + secretName + " should not be null", value, is(Matchers.notNullValue()));
            initialCaKeys.put(secretName, value);
            Secret annotated = new SecretBuilder(secret)
                    .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_FORCE_REPLACE, "true")
                    .endMetadata()
                    .build();
            LOGGER.info("Patching Secret: {}/{} with {}", testStorage.getNamespaceName(), secretName, Annotations.ANNO_STRIMZI_IO_FORCE_REPLACE);
            kubeClient().patchSecret(testStorage.getNamespaceName(), secretName, annotated);
        }

        for (int i = 1; i <= expectedRolls; i++) {
            // In case the first rolling update is finished, shut down cluster operator to see if the rolling update can be finished
            if (i == 2) {
                Pod coPod = kubeClient().listPodsByPrefixInName(clusterOperator.getDeploymentNamespace(), clusterOperator.getClusterOperatorName()).get(0);
                kubeClient().deletePod(clusterOperator.getDeploymentNamespace(), coPod);
            }
            if (!Environment.isKRaftModeEnabled()) {
                if (zkShouldRoll) {
                    LOGGER.info("Waiting for ZK rolling restart ({})", i);
                    controllerPods = i < expectedRolls ?
                            RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getControllerSelector(), controllerPods) :
                            RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getControllerSelector(), 3, controllerPods);
                }
            }

            if (kafkaShouldRoll) {
                LOGGER.info("Waiting for Kafka rolling restart ({})", i);
                brokerPods = i < expectedRolls ?
                        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), brokerPods) :
                        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);
            }

            if (eoShouldRoll) {
                LOGGER.info("Waiting for EO rolling restart ({})", i);
                eoPod = i < expectedRolls ?
                    DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), testStorage.getEoDeploymentName(), eoPod) :
                    DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), testStorage.getEoDeploymentName(), 1, eoPod);
            }

            if (keAndCCShouldRoll) {
                LOGGER.info("Waiting for KafkaExporter and CruiseControl rolling restart ({})", i);
                kePod = i < expectedRolls ?
                    DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), KafkaExporterResources.componentName(testStorage.getClusterName()), kePod) :
                    DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), KafkaExporterResources.componentName(testStorage.getClusterName()), 1, kePod);

                ccPod = i < expectedRolls ?
                    DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName()), ccPod) :
                    DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName()), 1, ccPod);
            }
        }

        LOGGER.info("Ensuring the certificates have been replaced");
        for (String secretName : secrets) {
            Secret secret = kubeClient().getSecret(testStorage.getNamespaceName(), secretName);
            assertThat("Secret " + secretName + " should exist", secret, is(notNullValue()));
            assertThat("CA key in " + secretName + " should have non-null 'data'", secret.getData(), is(notNullValue()));
            String value = secret.getData().get("ca.key");
            assertThat("CA key in " + secretName + " should exist", value, is(notNullValue()));
            assertThat("CA key in " + secretName + " should have changed",
                    value, is(not(initialCaKeys.get(secretName))));
        }

        kafkaClients.generateNewConsumerGroup();
        resourceManager.createResourceWithWait(kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        // Finally check a new client (signed by new client key) can consume

        final String bobUserName = "bobik-" + testStorage.getUsername();

        resourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), testStorage.getClusterName(), bobUserName).build());

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .withUsername(bobUserName)
            .build();

        resourceManager.createResourceWithWait(kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        if (!Environment.isKRaftModeEnabled()) {
            if (!zkShouldRoll) {
                assertThat("ZK Pods should not roll, but did.", PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector()), is(controllerPods));
            }
        }

        if (!kafkaShouldRoll) {
            assertThat("Kafka Pods should not roll, but did.", PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector()), is(brokerPods));
        }

        if (!eoShouldRoll) {
            assertThat("EO Pod should not roll, but did.", DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), testStorage.getEoDeploymentName()), is(eoPod));
        }

        if (!keAndCCShouldRoll) {
            assertThat("CC Pod should not roll, but did.", DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName())), is(ccPod));
            assertThat("KE Pod should not roll, but did.", DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), KafkaExporterResources.componentName(testStorage.getClusterName())), is(kePod));
        }
    }

    private void createKafkaCluster(TestStorage testStorage) {
        LOGGER.info("Creating a cluster");

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3)
                    .editSpec()
                        .withNewPersistentClaimStorage()
                            .withSize("2Gi")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endSpec()
                    .build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3)
                    .editSpec()
                        .withNewPersistentClaimStorage()
                            .withSize("2Gi")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endSpec()
                    .build()
            )
        );

        Kafka kafka = KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(TestConstants.PLAIN_LISTENER_DEFAULT_NAME)
                            .withPort(9092)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(false)
                            .build(),
                        new GenericKafkaListenerBuilder()
                            .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewKafkaListenerAuthenticationTlsAuth()
                            .endKafkaListenerAuthenticationTlsAuth()
                            .build())
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
                .withNewCruiseControl()
                .endCruiseControl()
                .withNewKafkaExporter()
                .endKafkaExporter()
            .endSpec()
            .build();

        if (Environment.isKRaftModeEnabled()) {
            kafka.getSpec().setZookeeper(null);
        }

        resourceManager.createResourceWithWait(kafka);
    }

    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    @Tag(CRUISE_CONTROL)
    void testAutoRenewCaCertsTriggerByExpiredCertificate() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        // 1. Create the Secrets already, and a certificate that's already expired
        InputStream secretInputStream = getClass().getClassLoader().getResourceAsStream("security-st-certs/expired-cluster-ca.crt");
        String clusterCaCert = TestUtils.readResource(secretInputStream);
        SecretUtils.createSecret(testStorage.getNamespaceName(), clusterCaCertificateSecretName(testStorage.getClusterName()), "ca.crt", clusterCaCert);

        // 2. Now create a cluster
        createKafkaCluster(testStorage);

        resourceManager.createResourceWithWait(
            KafkaUserTemplates.tlsUser(testStorage).build(),
            KafkaTopicTemplates.topic(testStorage).build()
        );

        final KafkaClients kafkaClients = ClientUtils.getInstantTlsClients(testStorage);
        resourceManager.createResourceWithWait(kafkaClients.producerTlsStrimzi(testStorage.getClusterName()), kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantClientSuccess(testStorage);

        // Wait until the certificates have been replaced
        SecretUtils.waitForCertToChange(testStorage.getNamespaceName(), clusterCaCert, clusterCaCertificateSecretName(testStorage.getClusterName()));

        // Wait until the pods are all up and ready
        KafkaUtils.waitForClusterStability(testStorage.getNamespaceName(), testStorage.getClusterName());

        resourceManager.createResourceWithWait(kafkaClients.producerTlsStrimzi(testStorage.getClusterName()), kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantClientSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    void testCertRenewalInMaintenanceTimeWindow() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final String clusterSecretName = KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName());
        final String clientsSecretName = KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName());

        LocalDateTime maintenanceWindowStart = LocalDateTime.now().withSecond(0);
        long maintenanceWindowDuration = 14;
        maintenanceWindowStart = maintenanceWindowStart.plusMinutes(15);
        final long windowStartMin = maintenanceWindowStart.getMinute();
        final long windowStopMin = windowStartMin + maintenanceWindowDuration > 59
                ? windowStartMin + maintenanceWindowDuration - 60 : windowStartMin + maintenanceWindowDuration;

        String maintenanceWindowCron = "* " + windowStartMin + "-" + windowStopMin + " * * * ? *";
        LOGGER.info("Initial maintenanceTimeWindow is: {}", maintenanceWindowCron);

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 1)
            .editSpec()
                .addToMaintenanceTimeWindows(maintenanceWindowCron)
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                        .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                        .withPort(9093)
                        .withType(KafkaListenerType.INTERNAL)
                        .withTls(true)
                        .withAuth(new KafkaListenerAuthenticationTls())
                    .build())
                .endKafka()
                .withNewClusterCa()
                    .withRenewalDays(15)
                    .withValidityDays(20)
                .endClusterCa()
                .withNewClientsCa()
                    .withRenewalDays(15)
                    .withValidityDays(20)
                .endClientsCa()
            .endSpec()
            .build());

        resourceManager.createResourceWithWait(
            KafkaUserTemplates.tlsUser(testStorage).build(),
            KafkaTopicTemplates.topic(testStorage).build()
        );

        Secret kafkaUserSecret = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), testStorage.getUsername());

        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        CertificateAuthority newCAValidity = new CertificateAuthority();
        newCAValidity.setRenewalDays(150);
        newCAValidity.setValidityDays(200);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(),
            k -> {
                k.getSpec().setClusterCa(newCAValidity);
                k.getSpec().setClientsCa(newCAValidity);
            },
            testStorage.getNamespaceName()
        );

        KafkaUtils.waitForKafkaStatusUpdate(testStorage.getNamespaceName(), testStorage.getClusterName());
        Secret secretCaCluster = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), clusterSecretName);
        Secret secretCaClients = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), clientsSecretName);
        assertThat("Cluster CA certificate has been renewed outside of maintenanceTimeWindows",
                secretCaCluster.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("0"));
        assertThat("Clients CA certificate has been renewed outside of maintenanceTimeWindows",
                secretCaClients.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("0"));

        assertThat("Rolling update was performed outside of maintenanceTimeWindows", brokerPods, is(PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector())));

        maintenanceWindowCron = "* " + LocalDateTime.now().getMinute() + "-" + windowStopMin + " * * * ? *";
        LOGGER.info("Set maintenanceTimeWindow to start now to save time: {}", maintenanceWindowCron);

        List<String> maintenanceTimeWindows = KafkaResource.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getSpec().getMaintenanceTimeWindows();
        maintenanceTimeWindows.add(maintenanceWindowCron);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> kafka.getSpec().setMaintenanceTimeWindows(maintenanceTimeWindows), testStorage.getNamespaceName());

        LOGGER.info("Waiting for rolling update to trigger during maintenanceTimeWindows");
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);

        Secret kafkaUserSecretRolled = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), testStorage.getUsername());
        secretCaCluster = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), clusterSecretName);
        secretCaClients = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), clientsSecretName);

        assertThat("Cluster CA certificate has not been renewed within maintenanceTimeWindows",
                secretCaCluster.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("1"));
        assertThat("Clients CA certificate has not been renewed within maintenanceTimeWindows",
                secretCaClients.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("1"));
        assertThat("KafkaUser certificate has not been renewed within maintenanceTimeWindows",
                kafkaUserSecret, not(sameInstance(kafkaUserSecretRolled)));

        final KafkaClients kafkaClients = ClientUtils.getInstantTlsClients(testStorage);
        resourceManager.createResourceWithWait(kafkaClients.producerTlsStrimzi(testStorage.getClusterName()), kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantClientSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    void testCertRegeneratedAfterInternalCAisDeleted() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 1).build());

        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        resourceManager.createResourceWithWait(
            KafkaUserTemplates.tlsUser(testStorage).build(),
            KafkaTopicTemplates.topic(testStorage).build()
        );

        List<Secret> secrets = kubeClient().listSecrets(testStorage.getNamespaceName()).stream()
            .filter(secret ->
                secret.getMetadata().getName().startsWith(testStorage.getClusterName()) &&
                secret.getMetadata().getName().endsWith("ca-cert"))
            .toList();

        for (Secret s : secrets) {
            LOGGER.info("Verifying that Secret: {}/{} with name {} is present", testStorage.getNamespaceName(), s, s.getMetadata().getName());
            assertThat(s.getData(), is(notNullValue()));
        }

        for (Secret s : secrets) {
            LOGGER.info("Deleting Secret: {}/{}", testStorage.getNamespaceName(), s.getMetadata().getName());
            kubeClient().deleteSecret(testStorage.getNamespaceName(), s.getMetadata().getName());
        }

        PodUtils.verifyThatRunningPodsAreStable(testStorage.getNamespaceName(), testStorage.getBrokerComponentName());
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);

        for (Secret s : secrets) {
            SecretUtils.waitForSecretReady(testStorage.getNamespaceName(), s.getMetadata().getName(), () -> { });
        }

        List<Secret> regeneratedSecrets = kubeClient().listSecrets(testStorage.getNamespaceName()).stream()
                .filter(secret -> secret.getMetadata().getName().endsWith("ca-cert"))
                .toList();

        for (int i = 0; i < secrets.size(); i++) {
            assertThat("Certificates has different cert UIDs", !secrets.get(i).getData().get("ca.crt").equals(regeneratedSecrets.get(i).getData().get("ca.crt")));
        }

        final KafkaClients kafkaClients = ClientUtils.getInstantTlsClients(testStorage);
        resourceManager.createResourceWithWait(kafkaClients.producerTlsStrimzi(testStorage.getClusterName()), kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantClientSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testTlsHostnameVerificationWithKafkaConnect() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3, 1).build());
        LOGGER.info("Getting IP of the bootstrap service");

        String ipOfBootstrapService = kubeClient(testStorage.getNamespaceName()).getService(testStorage.getNamespaceName(), KafkaResources.bootstrapServiceName(testStorage.getClusterName())).getSpec().getClusterIP();

        LOGGER.info("KafkaConnect without config {} will not connect to {}:9093", "ssl.endpoint.identification.algorithm", ipOfBootstrapService);

        resourceManager.createResourceWithoutWait(KafkaConnectTemplates.kafkaConnect(testStorage.getClusterName(), testStorage.getNamespaceName(), 1)
            .editSpec()
                .withNewTls()
                    .addNewTrustedCertificate()
                        .withSecretName(KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()))
                        .withCertificate("ca.crt")
                    .endTrustedCertificate()
                .endTls()
                .withBootstrapServers(ipOfBootstrapService + ":9093")
            .endSpec()
            .build());

        PodUtils.waitUntilPodIsPresent(testStorage.getNamespaceName(), testStorage.getClusterName() + "-connect");

        String kafkaConnectPodName = kubeClient(testStorage.getNamespaceName()).listPods(testStorage.getNamespaceName(), testStorage.getClusterName(), Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();

        PodUtils.waitUntilPodIsInCrashLoopBackOff(testStorage.getNamespaceName(), kafkaConnectPodName);

        assertThat("CrashLoopBackOff", is(kubeClient(testStorage.getNamespaceName()).getPod(testStorage.getNamespaceName(), kafkaConnectPodName).getStatus().getContainerStatuses()
                .get(0).getState().getWaiting().getReason()));

        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(testStorage.getClusterName(), kc -> {
            kc.getSpec().getConfig().put("ssl.endpoint.identification.algorithm", "");
        }, testStorage.getNamespaceName());

        LOGGER.info("KafkaConnect with config {} will connect to {}:9093", "ssl.endpoint.identification.algorithm", ipOfBootstrapService);

        KafkaConnectUtils.waitForConnectReady(testStorage.getNamespaceName(), testStorage.getClusterName());
    }

    @ParallelNamespaceTest
    @Tag(MIRROR_MAKER)
    void testTlsHostnameVerificationWithMirrorMaker() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceBrokerPoolName(), testStorage.getSourceClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceControllerPoolName(), testStorage.getSourceClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getSourceClusterName(), 1).build());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetBrokerPoolName(), testStorage.getTargetClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetControllerPoolName(), testStorage.getTargetClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getTargetClusterName(), 1).build());

        LOGGER.info("Getting IP of the source bootstrap service for consumer");
        String ipOfSourceBootstrapService = kubeClient(testStorage.getNamespaceName()).getService(testStorage.getNamespaceName(), KafkaResources.bootstrapServiceName(testStorage.getSourceClusterName())).getSpec().getClusterIP();

        LOGGER.info("Getting IP of the target bootstrap service for producer");
        String ipOfTargetBootstrapService = kubeClient(testStorage.getNamespaceName()).getService(testStorage.getNamespaceName(), KafkaResources.bootstrapServiceName(testStorage.getTargetClusterName())).getSpec().getClusterIP();

        LOGGER.info("KafkaMirrorMaker without config {} will not connect to consumer with address {}:9093", "ssl.endpoint.identification.algorithm", ipOfSourceBootstrapService);
        LOGGER.info("KafkaMirrorMaker without config {} will not connect to producer with address {}:9093", "ssl.endpoint.identification.algorithm", ipOfTargetBootstrapService);

        resourceManager.createResourceWithoutWait(KafkaMirrorMakerTemplates.kafkaMirrorMaker(testStorage.getClusterName(), testStorage.getSourceClusterName(), testStorage.getTargetClusterName(),
            ClientUtils.generateRandomConsumerGroup(), 1, true)
            .editSpec()
                .editConsumer()
                    .withNewTls()
                        .addNewTrustedCertificate()
                            .withSecretName(KafkaResources.clusterCaCertificateSecretName(testStorage.getSourceClusterName()))
                            .withCertificate("ca.crt")
                        .endTrustedCertificate()
                    .endTls()
                    .withBootstrapServers(ipOfSourceBootstrapService + ":9093")
                .endConsumer()
                .editProducer()
                    .withNewTls()
                        .addNewTrustedCertificate()
                            .withSecretName(KafkaResources.clusterCaCertificateSecretName(testStorage.getTargetClusterName()))
                            .withCertificate("ca.crt")
                        .endTrustedCertificate()
                    .endTls()
                    .withBootstrapServers(ipOfTargetBootstrapService + ":9093")
                .endProducer()
            .endSpec()
            .build());

        PodUtils.waitUntilPodIsPresent(testStorage.getNamespaceName(), testStorage.getClusterName() + "-mirror-maker");

        String kafkaMirrorMakerPodName = kubeClient(testStorage.getNamespaceName()).listPods(testStorage.getNamespaceName(), testStorage.getClusterName(), Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker.RESOURCE_KIND).get(0).getMetadata().getName();

        PodUtils.waitUntilPodIsInCrashLoopBackOff(testStorage.getNamespaceName(), kafkaMirrorMakerPodName);

        assertThat("CrashLoopBackOff", is(kubeClient(testStorage.getNamespaceName()).getPod(testStorage.getNamespaceName(), kafkaMirrorMakerPodName).getStatus().getContainerStatuses().get(0)
                .getState().getWaiting().getReason()));

        LOGGER.info("KafkaMirrorMaker with config {} will connect to consumer with address {}:9093", "ssl.endpoint.identification.algorithm", ipOfSourceBootstrapService);
        LOGGER.info("KafkaMirrorMaker with config {} will connect to producer with address {}:9093", "ssl.endpoint.identification.algorithm", ipOfTargetBootstrapService);

        LOGGER.info("Adding configuration {} to the MirrorMaker", "ssl.endpoint.identification.algorithm");
        KafkaMirrorMakerResource.replaceMirrorMakerResourceInSpecificNamespace(testStorage.getClusterName(), mm -> {
            mm.getSpec().getConsumer().getConfig().put("ssl.endpoint.identification.algorithm", ""); // disable hostname verification
            mm.getSpec().getProducer().getConfig().put("ssl.endpoint.identification.algorithm", ""); // disable hostname verification
        }, testStorage.getNamespaceName());

        KafkaMirrorMakerUtils.waitForKafkaMirrorMakerReady(testStorage.getNamespaceName(), testStorage.getClusterName());
    }

    @ParallelNamespaceTest
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    void testAclRuleReadAndWrite() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final String kafkaUserWrite = "kafka-user-write";
        final String kafkaUserRead = "kafka-user-read";
        final int numberOfMessages = 500;
        final String consumerGroupName = "consumer-group-name-1";

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3, 1)
            .editSpec()
                .editKafka()
                    .withNewKafkaAuthorizationSimple()
                    .endKafkaAuthorizationSimple()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9094)
                            .withType(KafkaListenerType.NODEPORT)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationTls())
                            .build())
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), testStorage.getNamespaceName()).build());
        resourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), testStorage.getClusterName(), kafkaUserWrite)
            .editSpec()
                .withNewKafkaUserAuthorizationSimple()
                    .addNewAcl()
                        .withNewAclRuleTopicResource()
                            .withName(testStorage.getTopicName())
                        .endAclRuleTopicResource()
                        .withOperations(AclOperation.WRITE, AclOperation.DESCRIBE, AclOperation.CREATE)
                    .endAcl()
                .endKafkaUserAuthorizationSimple()
            .endSpec()
            .build());

        LOGGER.info("Checking KafkaUser: {}/{} that is able to send messages to Topic: {}/{}", testStorage.getNamespaceName(), kafkaUserWrite, testStorage.getNamespaceName(), testStorage.getTopicName());

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(testStorage.getTopicName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withClusterName(testStorage.getClusterName())
            .withKafkaUsername(kafkaUserWrite)
            .withMessageCount(numberOfMessages)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        assertThat(externalKafkaClient.sendMessagesTls(), is(numberOfMessages));

        assertThrows(GroupAuthorizationException.class, externalKafkaClient::receiveMessagesTls);

        resourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), testStorage.getClusterName(), kafkaUserRead)
            .editSpec()
                .withNewKafkaUserAuthorizationSimple()
                    .addNewAcl()
                        .withNewAclRuleTopicResource()
                            .withName(testStorage.getTopicName())
                        .endAclRuleTopicResource()
                        .withOperations(AclOperation.READ, AclOperation.DESCRIBE)
                    .endAcl()
                    .addNewAcl()
                        .withNewAclRuleGroupResource()
                            .withName(consumerGroupName)
                        .endAclRuleGroupResource()
                        .withOperations(AclOperation.READ)
                    .endAcl()
                .endKafkaUserAuthorizationSimple()
            .endSpec()
            .build());

        ExternalKafkaClient newExternalKafkaClient = externalKafkaClient.toBuilder()
            .withKafkaUsername(kafkaUserRead)
            .withConsumerGroupName(consumerGroupName)
            .build();

        assertThat(newExternalKafkaClient.receiveMessagesTls(), is(numberOfMessages));

        LOGGER.info("Checking KafkaUser: {}/{} that is not able to send messages to Topic: {}/{}", testStorage.getNamespaceName(), kafkaUserRead, testStorage.getNamespaceName(), testStorage.getTopicName());
        assertThrows(Exception.class, newExternalKafkaClient::sendMessagesTls);
    }

    @ParallelNamespaceTest
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    void testAclWithSuperUser() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3, 1)
            .editSpec()
                .editKafka()
                    .withNewKafkaAuthorizationSimple()
                        .withSuperUsers("CN=" + testStorage.getKafkaUsername())
                    .endKafkaAuthorizationSimple()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9094)
                            .withType(KafkaListenerType.NODEPORT)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationTls())
                            .build())
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), testStorage.getNamespaceName()).build());
        resourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), testStorage.getClusterName(), testStorage.getKafkaUsername())
            .editSpec()
                .withNewKafkaUserAuthorizationSimple()
                    .addNewAcl()
                        .withNewAclRuleTopicResource()
                            .withName(testStorage.getTopicName())
                        .endAclRuleTopicResource()
                        .withOperations(AclOperation.WRITE, AclOperation.DESCRIBE)
                    .endAcl()
                .endKafkaUserAuthorizationSimple()
            .endSpec()
            .build());

        LOGGER.info("Checking Kafka super user: {}/{} that is able to send messages to Topic: {}", testStorage.getNamespaceName(), testStorage.getKafkaUsername(), testStorage.getTopicName());

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(testStorage.getTopicName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withClusterName(testStorage.getClusterName())
            .withKafkaUsername(testStorage.getKafkaUsername())
            .withMessageCount(testStorage.getMessageCount())
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        assertThat(externalKafkaClient.sendMessagesTls(), is(testStorage.getMessageCount()));

        LOGGER.info("Checking Kafka super user: {}/{} that is able to read messages to Topic: {}/{} regardless that " +
                "we configured Acls with only write operation", testStorage.getNamespaceName(), testStorage.getKafkaUsername(), testStorage.getNamespaceName(), testStorage.getTopicName());

        assertThat(externalKafkaClient.receiveMessagesTls(), is(testStorage.getMessageCount()));

        String nonSuperuserName = testStorage.getKafkaUsername() + "-non-super-user";

        resourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), testStorage.getClusterName(), nonSuperuserName)
            .editSpec()
                .withNewKafkaUserAuthorizationSimple()
                    .addNewAcl()
                        .withNewAclRuleTopicResource()
                            .withName(testStorage.getTopicName())
                        .endAclRuleTopicResource()
                        .withOperations(AclOperation.WRITE, AclOperation.DESCRIBE)
                    .endAcl()
                .endKafkaUserAuthorizationSimple()
            .endSpec()
            .build());

        LOGGER.info("Checking Kafka super user: {}/{} that is able to send messages to Topic: {}/{}", testStorage.getNamespaceName(), nonSuperuserName, testStorage.getNamespaceName(), testStorage.getTopicName());

        externalKafkaClient = externalKafkaClient.toBuilder()
            .withKafkaUsername(nonSuperuserName)
            .build();

        assertThat(externalKafkaClient.sendMessagesTls(), is(testStorage.getMessageCount()));

        LOGGER.info("Checking Kafka super user: {}/{} that is not able to read messages to Topic: {}/{} because of defined" +
                " ACLs on only write operation", testStorage.getNamespaceName(), nonSuperuserName, testStorage.getNamespaceName(), testStorage.getTopicName());

        ExternalKafkaClient newExternalKafkaClient = externalKafkaClient.toBuilder()
            .withConsumerGroupName(ClientUtils.generateRandomConsumerGroup())
            .build();

        assertThrows(GroupAuthorizationException.class, newExternalKafkaClient::receiveMessagesTls);
    }

    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    void testCaRenewalBreakInMiddle() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        // Extra Kafka configuration ensures that topic created automatically (by producer) will have data available on more than just single replica once one of broker fails
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 3)
            .editSpec()
                .withNewClusterCa()
                    .withRenewalDays(1)
                    .withValidityDays(3)
                .endClusterCa()
                .editOrNewKafka()
                    .addToConfig("default.replication.factor", 3)
                    .addToConfig("min.insync.replicas", 2)
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResourceWithWait(
            KafkaUserTemplates.tlsUser(testStorage).build(),
            KafkaTopicTemplates.topic(testStorage)
                .editOrNewSpec()
                    .withReplicas(3)
                .endSpec()
                .build()
        );

        KafkaClients kafkaClients = ClientUtils.getInstantTlsClients(testStorage);
        resourceManager.createResourceWithWait(kafkaClients.producerTlsStrimzi(testStorage.getClusterName()), kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantClientSuccess(testStorage);

        Map<String, String> controllerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());
        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        Map<String, String> eoPods = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), testStorage.getEoDeploymentName());

        final String clusterCaCert = kubeClient().getSecret(testStorage.getNamespaceName(), KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName())).getData().get("ca.crt");

        final ResourceRequirements requirements = new ResourceRequirementsBuilder()
            .addToRequests("cpu", new Quantity("100000m"))
            .build();

        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getBrokerPoolName(), knp -> knp.getSpec().setResources(requirements), testStorage.getNamespaceName());
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> k.getSpec()
                    .getKafka()
                    .setResources(requirements),
                testStorage.getNamespaceName());
        }
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(),
            k -> k.getSpec().setClusterCa(new CertificateAuthorityBuilder()
                .withRenewalDays(4)
                .withValidityDays(7)
                .build()), testStorage.getNamespaceName());

        TestUtils.waitFor("any Kafka Pod to be in the pending phase because of selected high cpu resource",
            TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> {
                List<Pod> pendingPods = kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getBrokerComponentName())
                    .stream().filter(pod -> pod.getStatus().getPhase().equals("Pending")).collect(Collectors.toList());
                if (pendingPods.isEmpty()) {
                    LOGGER.info("No Pods of {} are in desired state", testStorage.getBrokerComponentName());
                    return false;
                } else {
                    LOGGER.info("Pod in 'Pending' state: {}", pendingPods.get(0).getMetadata().getName());
                    return true;
                }
            }
        );

        kafkaClients.generateNewConsumerGroup();
        resourceManager.createResourceWithWait(kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        final ResourceRequirements correctRequirements = new ResourceRequirementsBuilder()
            .addToRequests("cpu", new Quantity("200m"))
            .build();

        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getBrokerPoolName(),
                knp -> knp.getSpec().setResources(correctRequirements), testStorage.getNamespaceName());
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> {
                k.getSpec()
                    .getKafka()
                    .setResources(new ResourceRequirementsBuilder()
                        .addToRequests("cpu", new Quantity("200m"))
                        .build());
            }, testStorage.getNamespaceName());
        }

        // Wait for the certificates to get replaced
        SecretUtils.waitForCertToChange(testStorage.getNamespaceName(), clusterCaCert, KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()));
        if (!Environment.isKRaftModeEnabled()) {
            RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getControllerSelector(), 3, controllerPods);
        }
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);
        DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), testStorage.getEoDeploymentName(), 1, eoPods);

        kafkaClients.generateNewConsumerGroup();
        resourceManager.createResourceWithWait(kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        // Try to send and receive messages with new certificates
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getClusterName(), topicName, testStorage.getNamespaceName()).build());

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .withTopicName(topicName)
            .build();

        resourceManager.createResourceWithWait(kafkaClients.producerTlsStrimzi(testStorage.getClusterName()), kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantClientSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testKafkaAndKafkaConnectTlsVersion() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final Map<String, Object> configWithNewestVersionOfTls = new HashMap<>();

        final String tlsVersion12 = "TLSv1.2";
        final String tlsVersion1 = "TLSv1";

        configWithNewestVersionOfTls.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, tlsVersion12);
        configWithNewestVersionOfTls.put(SslConfigs.SSL_PROTOCOL_CONFIG, SslConfigs.DEFAULT_SSL_PROTOCOL);

        LOGGER.info("Deploying Kafka cluster with the support {} TLS",  tlsVersion12);

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withConfig(configWithNewestVersionOfTls)
                .endKafka()
            .endSpec()
            .build());

        Map<String, Object> configsFromKafkaCustomResource = KafkaResource.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getSpec().getKafka().getConfig();

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

        resourceManager.createResourceWithoutWait(KafkaConnectTemplates.kafkaConnect(testStorage.getClusterName(), testStorage.getNamespaceName(), 1)
            .editSpec()
                .withConfig(configWithLowestVersionOfTls)
            .endSpec()
            .build());

        LOGGER.info("Verifying that KafkaConnect status is NotReady because of different TLS version");

        KafkaConnectUtils.waitForConnectNotReady(testStorage.getNamespaceName(), testStorage.getClusterName());

        LOGGER.info("Replacing KafkaConnect config to the newest(TLSv1.2) one same as the Kafka Broker has");

        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(testStorage.getClusterName(), kafkaConnect -> kafkaConnect.getSpec().setConfig(configWithNewestVersionOfTls), testStorage.getNamespaceName());

        LOGGER.info("Verifying that KafkaConnect has the accepted configuration:\n {} -> {}\n {} -> {}",
                SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG,
                tlsVersion12,
                SslConfigs.SSL_PROTOCOL_CONFIG,
                SslConfigs.DEFAULT_SSL_PROTOCOL);

        KafkaConnectUtils.waitForKafkaConnectConfigChange(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, tlsVersion12, testStorage.getNamespaceName(), testStorage.getClusterName());
        KafkaConnectUtils.waitForKafkaConnectConfigChange(SslConfigs.SSL_PROTOCOL_CONFIG, SslConfigs.DEFAULT_SSL_PROTOCOL, testStorage.getNamespaceName(), testStorage.getClusterName());

        LOGGER.info("Verifying that KafkaConnect is stable");

        PodUtils.verifyThatRunningPodsAreStable(testStorage.getNamespaceName(), KafkaConnectResources.componentName(testStorage.getClusterName()));

        LOGGER.info("Verifying that KafkaConnect status is Ready because of same TLS version");

        KafkaConnectUtils.waitForConnectReady(testStorage.getNamespaceName(), testStorage.getClusterName());
    }

    @ParallelNamespaceTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testKafkaAndKafkaConnectCipherSuites() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final Map<String, Object> configWithCipherSuitesSha384 = new HashMap<>();

        final String cipherSuitesSha384 = "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384";
        final String cipherSuitesSha256 = "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256";

        configWithCipherSuitesSha384.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, cipherSuitesSha384);

        LOGGER.info("Deploying Kafka cluster with the support {} cipher algorithms",  cipherSuitesSha384);

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withConfig(configWithCipherSuitesSha384)
                .endKafka()
            .endSpec()
            .build());

        Map<String, Object> configsFromKafkaCustomResource = KafkaResource.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getSpec().getKafka().getConfig();

        LOGGER.info("Verifying that KafkaConnect has the accepted configuration:\n {} -> {}",
                SslConfigs.SSL_CIPHER_SUITES_CONFIG, configsFromKafkaCustomResource.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG));

        assertThat(configsFromKafkaCustomResource.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG), is(cipherSuitesSha384));

        Map<String, Object> configWithCipherSuitesSha256 = new HashMap<>();

        configWithCipherSuitesSha256.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, cipherSuitesSha256);

        resourceManager.createResourceWithoutWait(KafkaConnectTemplates.kafkaConnect(testStorage.getClusterName(), testStorage.getNamespaceName(), 1)
            .editSpec()
                .withConfig(configWithCipherSuitesSha256)
            .endSpec()
            .build());

        LOGGER.info("Verifying that KafkaConnect status is NotReady because of different cipher suites complexity of algorithm");

        KafkaConnectUtils.waitForConnectNotReady(testStorage.getNamespaceName(), testStorage.getClusterName());

        LOGGER.info("Replacing KafkaConnect config to the cipher suites same as the Kafka Broker has");

        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(testStorage.getClusterName(), kafkaConnect -> kafkaConnect.getSpec().setConfig(configWithCipherSuitesSha384), testStorage.getNamespaceName());

        LOGGER.info("Verifying that KafkaConnect has the accepted configuration:\n {} -> {}",
                SslConfigs.SSL_CIPHER_SUITES_CONFIG, configsFromKafkaCustomResource.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG));

        KafkaConnectUtils.waitForKafkaConnectConfigChange(SslConfigs.SSL_CIPHER_SUITES_CONFIG, cipherSuitesSha384, testStorage.getNamespaceName(), testStorage.getClusterName());

        LOGGER.info("Verifying that KafkaConnect is stable");

        PodUtils.verifyThatRunningPodsAreStable(testStorage.getNamespaceName(), KafkaConnectResources.componentName(testStorage.getClusterName()));

        LOGGER.info("Verifying that KafkaConnect status is Ready because of the same cipher suites complexity of algorithm");

        KafkaConnectUtils.waitForConnectReady(testStorage.getNamespaceName(), testStorage.getClusterName());
    }

    @ParallelNamespaceTest
    void testOwnerReferenceOfCASecrets() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        /* Different name for Kafka cluster to make the test quicker -> KafkaRoller is waiting for pods of "my-cluster" to become ready
         for 5 minutes -> this will prevent the waiting. */
        final String secondClusterName = "my-second-cluster-" + testStorage.getClusterName();

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
            .editOrNewSpec()
                .withNewClusterCa()
                    .withGenerateSecretOwnerReference(false)
                .endClusterCa()
                .withNewClientsCa()
                    .withGenerateSecretOwnerReference(false)
                .endClientsCa()
            .endSpec()
            .build());

        LOGGER.info("Listing all cluster CAs for {}", testStorage.getClusterName());
        List<Secret> caSecrets = kubeClient(testStorage.getNamespaceName()).listSecrets(testStorage.getNamespaceName()).stream()
            .filter(secret -> secret.getMetadata().getName().contains(KafkaResources.clusterCaKeySecretName(testStorage.getClusterName())) || secret.getMetadata().getName().contains(KafkaResources.clientsCaKeySecretName(testStorage.getClusterName()))).collect(Collectors.toList());

        LOGGER.info("Deleting Kafka: {}", testStorage.getClusterName());
        KafkaResource.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.kafkaNodePoolClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getBrokerPoolName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
            if (Environment.isKRaftModeEnabled()) {
                KafkaNodePoolResource.kafkaNodePoolClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getControllerPoolName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
            }
        }
        KafkaUtils.waitForKafkaDeletion(testStorage.getNamespaceName(), testStorage.getClusterName());

        LOGGER.info("Checking actual Secrets after Kafka deletion");
        caSecrets.forEach(caSecret -> {
            String secretName = caSecret.getMetadata().getName();
            LOGGER.info("Checking that Secret: {} is still present", secretName);
            assertNotNull(kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), secretName));

            LOGGER.info("Deleting Secret: {}/{}", testStorage.getNamespaceName(), secretName);
            kubeClient(testStorage.getNamespaceName()).deleteSecret(testStorage.getNamespaceName(), secretName);
        });

        LOGGER.info("Deploying Kafka with generateSecretOwnerReference set to true");
        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), secondClusterName, 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), secondClusterName, 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(secondClusterName, 3)
            .editOrNewSpec()
                .editOrNewClusterCa()
                    .withGenerateSecretOwnerReference(true)
                .endClusterCa()
                .editOrNewClientsCa()
                    .withGenerateSecretOwnerReference(true)
                .endClientsCa()
            .endSpec()
            .build());

        caSecrets = kubeClient(testStorage.getNamespaceName()).listSecrets(testStorage.getNamespaceName()).stream()
            .filter(secret -> secret.getMetadata().getName().contains(KafkaResources.clusterCaKeySecretName(secondClusterName)) || secret.getMetadata().getName().contains(KafkaResources.clientsCaKeySecretName(secondClusterName))).collect(Collectors.toList());

        LOGGER.info("Deleting Kafka: {}", secondClusterName);
        KafkaResource.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(secondClusterName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
        KafkaUtils.waitForKafkaDeletion(testStorage.getNamespaceName(), secondClusterName);

        LOGGER.info("Checking actual Secrets after Kafka deletion");
        caSecrets.forEach(caSecret -> {
            String secretName = caSecret.getMetadata().getName();
            LOGGER.info("Checking that Secret: {} is deleted", secretName);
            TestUtils.waitFor("deletion of Secret: " + secretName, TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
                () -> kubeClient().getSecret(testStorage.getNamespaceName(), secretName) == null);
        });
    }

    @ParallelNamespaceTest
    void testClusterCACertRenew() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
            .editOrNewSpec()
                .withNewClusterCa()
                    .withRenewalDays(15)
                    .withValidityDays(20)
                .endClusterCa()
            .endSpec()
            .build());

        String brokerPodName = kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector()).get(0).getMetadata().getName();
        final Map<String, String> controllerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());
        final Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        final Map<String, String> eoPod = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), testStorage.getEoDeploymentName());

        Secret clusterCASecret = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()));
        X509Certificate cacert = SecretUtils.getCertificateFromSecret(clusterCASecret, "ca.crt");
        Date initialCertStartTime = cacert.getNotBefore();
        Date initialCertEndTime = cacert.getNotAfter();

        // Check Broker kafka certificate dates
        Secret brokerCertCreationSecret = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), testStorage.getClusterName() + "-kafka-brokers");
        X509Certificate kafkaBrokerCert = SecretUtils.getCertificateFromSecret(brokerCertCreationSecret,
            brokerPodName + ".crt");
        Date initialKafkaBrokerCertStartTime = kafkaBrokerCert.getNotBefore();
        Date initialKafkaBrokerCertEndTime = kafkaBrokerCert.getNotAfter();

        Date initialZkCertStartTime = null;
        Date initialZkCertEndTime = null;
        Secret zkCertCreationSecret = null;
        X509Certificate zkBrokerCert = null;
        if (!Environment.isKRaftModeEnabled()) {
            // Check Zookeeper certificate dates
            zkCertCreationSecret = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), testStorage.getClusterName() + "-zookeeper-nodes");
            zkBrokerCert = SecretUtils.getCertificateFromSecret(zkCertCreationSecret, testStorage.getClusterName() + "-zookeeper-0.crt");
            initialZkCertStartTime = zkBrokerCert.getNotBefore();
            initialZkCertEndTime = zkBrokerCert.getNotAfter();
        }

        LOGGER.info("Change of Kafka validity and renewal days - reconciliation should start");
        CertificateAuthority newClusterCA = new CertificateAuthority();
        newClusterCA.setRenewalDays(150);
        newClusterCA.setValidityDays(200);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> k.getSpec().setClusterCa(newClusterCA), testStorage.getNamespaceName());

        // On the next reconciliation, the Cluster Operator performs a `rolling update`:
        //   a) ZooKeeper
        //   b) Kafka
        //   c) and other components to trust the new Cluster CA certificate. (i.e., Entity Operator)
        if (!Environment.isKRaftModeEnabled()) {
            RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getControllerSelector(), 3, controllerPods);
        }
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);
        DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), testStorage.getEoDeploymentName(), 1, eoPod);

        // Read renewed secret/certs again
        clusterCASecret = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()));
        cacert = SecretUtils.getCertificateFromSecret(clusterCASecret, "ca.crt");
        Date changedCertStartTime = cacert.getNotBefore();
        Date changedCertEndTime = cacert.getNotAfter();

        // Check renewed Broker kafka certificate dates
        brokerCertCreationSecret = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), testStorage.getClusterName() + "-kafka-brokers");
        kafkaBrokerCert = SecretUtils.getCertificateFromSecret(brokerCertCreationSecret, brokerPodName + ".crt");
        Date changedKafkaBrokerCertStartTime = kafkaBrokerCert.getNotBefore();
        Date changedKafkaBrokerCertEndTime = kafkaBrokerCert.getNotAfter();

        LOGGER.info("Initial ClusterCA cert dates: " + initialCertStartTime + " --> " + initialCertEndTime);
        LOGGER.info("Changed ClusterCA cert dates: " + changedCertStartTime + " --> " + changedCertEndTime);
        LOGGER.info("Kafka Broker cert creation dates: " + initialKafkaBrokerCertStartTime + " --> " + initialKafkaBrokerCertEndTime);
        LOGGER.info("Kafka Broker cert changed dates:  " + changedKafkaBrokerCertStartTime + " --> " + changedKafkaBrokerCertEndTime);

        String msg = "Error: original cert-end date: '" + initialCertEndTime +
                "' ends sooner than changed (prolonged) cert date '" + changedCertEndTime + "'!";
        assertThat(msg, initialCertEndTime.compareTo(changedCertEndTime) < 0);
        assertThat("Broker certificates start dates have not been renewed.",
                initialKafkaBrokerCertStartTime.compareTo(changedKafkaBrokerCertStartTime) < 0);
        assertThat("Broker certificates end dates have not been renewed.",
                initialKafkaBrokerCertEndTime.compareTo(changedKafkaBrokerCertEndTime) < 0);

        if (!Environment.isKRaftModeEnabled()) {
            // Check renewed Zookeeper certificate dates
            zkCertCreationSecret = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), testStorage.getClusterName() + "-zookeeper-nodes");
            zkBrokerCert = SecretUtils.getCertificateFromSecret(zkCertCreationSecret, testStorage.getClusterName() + "-zookeeper-0.crt");
            Date changedZkCertStartTime = zkBrokerCert.getNotBefore();
            Date changedZkCertEndTime = zkBrokerCert.getNotAfter();

            LOGGER.info("ZooKeeper cert creation dates: " + initialZkCertStartTime + " --> " + initialZkCertEndTime);
            LOGGER.info("ZooKeeper cert changed dates:  " + changedZkCertStartTime + " --> " + changedZkCertEndTime);

            assertThat("ZooKeeper certificates start dates have not been renewed.",
                    initialZkCertStartTime.compareTo(changedZkCertStartTime) < 0);
            assertThat("ZooKeeper certificates end dates have not been renewed.",
                    initialZkCertEndTime.compareTo(changedZkCertEndTime) < 0);
        }
    }

    @ParallelNamespaceTest
    void testClientsCACertRenew() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
            .editOrNewSpec()
                .withNewClientsCa()
                    .withRenewalDays(15)
                    .withValidityDays(20)
                .endClientsCa()
            .endSpec()
            .build());

        String username = "strimzi-tls-user-" + new Random().nextInt(Integer.MAX_VALUE);
        resourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), testStorage.getClusterName(), username).build());

        final Map<String, String> controllerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());
        final Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        final Map<String, String> eoPod = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), testStorage.getEoDeploymentName());

        // Check initial clientsCA validity days
        Secret clientsCASecret = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName()));
        X509Certificate cacert = SecretUtils.getCertificateFromSecret(clientsCASecret, "ca.crt");
        Date initialCertStartTime = cacert.getNotBefore();
        Date initialCertEndTime = cacert.getNotAfter();

        // Check initial kafkauser validity days
        X509Certificate userCert = SecretUtils.getCertificateFromSecret(kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), username), "user.crt");
        Date initialKafkaUserCertStartTime = userCert.getNotBefore();
        Date initialKafkaUserCertEndTime = userCert.getNotAfter();

        LOGGER.info("Change of Kafka validity and renewal days - reconciliation should start");
        CertificateAuthority newClientsCA = new CertificateAuthority();
        newClientsCA.setRenewalDays(150);
        newClientsCA.setValidityDays(200);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> k.getSpec().setClientsCa(newClientsCA), testStorage.getNamespaceName());

        // On the next reconciliation, the Cluster Operator performs a `rolling update` only for the
        // `Kafka pods`.
        // a) ZooKeeper must not roll
        if (!Environment.isKRaftModeEnabled()) {
            RollingUpdateUtils.waitForNoRollingUpdate(testStorage.getNamespaceName(), testStorage.getControllerSelector(), controllerPods);
        }

        // b) Kafka has to roll
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);

        // c) EO must roll (because User Operator uses Clients CA for issuing user certificates)
        DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), testStorage.getEoDeploymentName(), 1, eoPod);

        // Read renewed secret/certs again
        clientsCASecret = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName()));
        cacert = SecretUtils.getCertificateFromSecret(clientsCASecret, "ca.crt");
        Date changedCertStartTime = cacert.getNotBefore();
        Date changedCertEndTime = cacert.getNotAfter();

        userCert = SecretUtils.getCertificateFromSecret(kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), username), "user.crt");
        Date changedKafkaUserCertStartTime = userCert.getNotBefore();
        Date changedKafkaUserCertEndTime = userCert.getNotAfter();

        LOGGER.info("Initial ClientsCA cert dates: " + initialCertStartTime + " --> " + initialCertEndTime);
        LOGGER.info("Changed ClientsCA cert dates: " + changedCertStartTime + " --> " + changedCertEndTime);
        LOGGER.info("Initial userCert dates: " + initialKafkaUserCertStartTime + " --> " + initialKafkaUserCertEndTime);
        LOGGER.info("Changed userCert dates: " + changedKafkaUserCertStartTime + " --> " + changedKafkaUserCertEndTime);


        String msg = "Error: original cert-end date: '" + initialCertEndTime +
                "' ends sooner than changed (prolonged) cert date '" + changedCertEndTime + "'";
        assertThat(msg, initialCertEndTime.compareTo(changedCertEndTime) < 0);
        assertThat("UserCert start date has been renewed",
                initialKafkaUserCertStartTime.compareTo(changedKafkaUserCertStartTime) < 0);
        assertThat("UserCert end date has been renewed",
                initialKafkaUserCertEndTime.compareTo(changedKafkaUserCertEndTime) < 0);
    }

    @BeforeAll
    void setup() {
        this.clusterOperator = this.clusterOperator
                .defaultInstallation()
                .createInstallation()
                .runInstallation();
    }
}
