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
import io.fabric8.kubernetes.client.dsl.base.PatchContext;
import io.fabric8.kubernetes.client.dsl.base.PatchType;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.common.CertificateAuthority;
import io.strimzi.api.kafka.model.common.CertificateAuthorityBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlResources;
import io.strimzi.api.kafka.model.kafka.exporter.KafkaExporterResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.user.acl.AclOperation;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.kafkaclients.ClientsAuthentication;
import io.strimzi.systemtest.kafkaclients.externalClients.ExternalKafkaClient;
import io.strimzi.systemtest.resources.CrdClients;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaNodePoolUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.test.ReadWriteUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.testclients.clients.kafka.KafkaProducerConsumer;
import io.strimzi.testclients.clients.kafka.KafkaProducerConsumerBuilder;
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
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static io.strimzi.api.kafka.model.kafka.KafkaResources.clientsCaCertificateSecretName;
import static io.strimzi.api.kafka.model.kafka.KafkaResources.clientsCaKeySecretName;
import static io.strimzi.api.kafka.model.kafka.KafkaResources.clusterCaCertificateSecretName;
import static io.strimzi.api.kafka.model.kafka.KafkaResources.clusterCaKeySecretName;
import static io.strimzi.systemtest.TestTags.ACCEPTANCE;
import static io.strimzi.systemtest.TestTags.CONNECT;
import static io.strimzi.systemtest.TestTags.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.TestTags.CRUISE_CONTROL;
import static io.strimzi.systemtest.TestTags.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.TestTags.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.TestTags.ROLLING_UPDATE;
import static io.strimzi.systemtest.TestTags.SANITY;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(REGRESSION)
@SuiteDoc(
    description = @Desc("Test suite for verifying TLS certificate management, CA renewal and replacement, certificate maintenance windows, ACL authorization, and TLS configuration for Kafka and KafkaConnect components."),
    labels = {
        @Label(value = TestDocsLabels.SECURITY)
    }
)
class SecurityST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(SecurityST.class);

    @TestDoc(
        description = @Desc("Test verifying that annotating the cluster CA certificate secret with strimzi.io/force-renew triggers automatic renewal of the cluster CA certificate, causing rolling updates of Kafka brokers, controllers, Entity Operator, KafkaExporter, and Cruise Control."),
        steps = {
            @Step(value = "Deploy Kafka cluster with TLS listener, Cruise Control, and KafkaExporter.", expected = "Kafka cluster is deployed."),
            @Step(value = "Create KafkaUser and KafkaTopic, produce and consume messages over TLS.", expected = "Messages are exchanged successfully."),
            @Step(value = "Annotate the cluster CA certificate secret with strimzi.io/force-renew.", expected = "CA cert renewal is triggered."),
            @Step(value = "Wait for rolling updates of Kafka, Entity Operator, KafkaExporter, and Cruise Control.", expected = "All components roll."),
            @Step(value = "Verify the CA certificate has changed and clients can still consume messages.", expected = "New certificate is in use and messaging works.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    @ParallelNamespaceTest
    @Tag(ROLLING_UPDATE)
    @Tag(CRUISE_CONTROL)
    @Tag("ClusterCaCerts")
    void testAutoRenewClusterCaCertsTriggeredByAnno() {
        autoRenewSomeCaCertsTriggeredByAnno(
                /* brokers need new certs */
                true,
                /* eo needs new cert */
                true,
                true);
    }

    @TestDoc(
        description = @Desc("Test verifying that annotating the clients CA certificate secret with strimzi.io/force-renew triggers automatic renewal of the clients CA certificate, causing rolling updates of Kafka brokers without rolling Entity Operator, KafkaExporter, or Cruise Control."),
        steps = {
            @Step(value = "Deploy Kafka cluster with TLS listener, Cruise Control, and KafkaExporter.", expected = "Kafka cluster is deployed."),
            @Step(value = "Create KafkaUser and KafkaTopic, produce and consume messages over TLS.", expected = "Messages are exchanged successfully."),
            @Step(value = "Annotate the clients CA certificate secret with strimzi.io/force-renew.", expected = "CA cert renewal is triggered."),
            @Step(value = "Wait for Kafka rolling update; verify Entity Operator, KafkaExporter, and Cruise Control do not roll.", expected = "Only Kafka rolls."),
            @Step(value = "Verify the CA certificate has changed and clients can still consume messages.", expected = "New certificate is in use and messaging works.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    @ParallelNamespaceTest
    @Tag(ROLLING_UPDATE)
    @Tag(CRUISE_CONTROL)
    @Tag("ClientsCaCerts")
    void testAutoRenewClientsCaCertsTriggeredByAnno() {
        autoRenewSomeCaCertsTriggeredByAnno(
                /* brokers need to trust client certs with new cert */
                true,
                /* eo needs to generate new client certs */
                false,
                false);
    }

    @TestDoc(
        description = @Desc("Test verifying that annotating both cluster and clients CA certificate secrets with strimzi.io/force-renew triggers automatic renewal of all CA certificates, causing rolling updates of all components."),
        steps = {
            @Step(value = "Deploy Kafka cluster with TLS listener, Cruise Control, and KafkaExporter.", expected = "Kafka cluster is deployed."),
            @Step(value = "Create KafkaUser and KafkaTopic, produce and consume messages over TLS.", expected = "Messages are exchanged successfully."),
            @Step(value = "Annotate both cluster and clients CA certificate secrets with strimzi.io/force-renew.", expected = "CA cert renewal is triggered for both CAs."),
            @Step(value = "Wait for rolling updates of all components.", expected = "All components roll."),
            @Step(value = "Verify both CA certificates have changed and clients can still consume messages.", expected = "New certificates are in use and messaging works.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    @ParallelNamespaceTest
    @Tag(SANITY)
    @Tag(ACCEPTANCE)
    @Tag(ROLLING_UPDATE)
    @Tag(CRUISE_CONTROL)
    @Tag("AllCaCerts")
    void testAutoRenewAllCaCertsTriggeredByAnno() {
        autoRenewSomeCaCertsTriggeredByAnno(
                true,
                true,
                true);
    }

    @SuppressWarnings({"checkstyle:MethodLength", "checkstyle:NPathComplexity"})
    void autoRenewSomeCaCertsTriggeredByAnno(
            boolean kafkaShouldRoll,
            boolean eoShouldRoll,
            boolean keAndCCShouldRoll) {

        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        createKafkaCluster(testStorage);

        List<String> secrets;

        // to make it parallel we need decision maker...
        if (KubeResourceManager.get().getTestContext().getTags().contains("ClusterCaCerts")) {
            secrets = Arrays.asList(clusterCaCertificateSecretName(testStorage.getClusterName()));
        } else if (KubeResourceManager.get().getTestContext().getTags().contains("ClientsCaCerts")) {
            secrets = Arrays.asList(clientsCaCertificateSecretName(testStorage.getClusterName()));
        } else {
            // AllCaKeys
            secrets = Arrays.asList(clusterCaCertificateSecretName(testStorage.getClusterName()),
                clientsCaCertificateSecretName(testStorage.getClusterName()));
        }

        KubeResourceManager.get().createResourceWithWait(
            KafkaUserTemplates.tlsUser(testStorage).build(),
            KafkaTopicTemplates.topic(testStorage).build()
        );

        final KafkaProducerConsumer kafkaProducerConsumer = new KafkaProducerConsumerBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withMessageCount(testStorage.getMessageCount())
            .withAuthentication(ClientsAuthentication.configureTls(testStorage.getClusterName(), testStorage.getUsername()))
            .build();

        KubeResourceManager.get().createResourceWithWait(
            kafkaProducerConsumer.getProducer().getJob(),
            kafkaProducerConsumer.getConsumer().getJob()
        );

        ClientUtils.waitForClientsSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getProducerName(), testStorage.getMessageCount());

        // Get all pods, and their resource versions
        Map<String, String> controllerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());
        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        Map<String, String> eoPod = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), testStorage.getEoDeploymentName());
        Map<String, String> ccPod = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName()));
        Map<String, String> kePod = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), KafkaExporterResources.componentName(testStorage.getClusterName()));

        LOGGER.info("Triggering CA cert renewal by adding the annotation");
        Map<String, String> initialCaCerts = new HashMap<>();
        for (String secretName : secrets) {
            Secret secret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(secretName).get();
            String value = secret.getData().get("ca.crt");
            assertThat("ca.crt in " + secretName + " should not be null", value, is(notNullValue()));
            initialCaCerts.put(secretName, value);
            Secret annotated = new SecretBuilder(secret)
                .editMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_FORCE_RENEW, "true")
                .endMetadata()
                .build();
            LOGGER.info("Patching Secret: {} with {}", secretName, Annotations.ANNO_STRIMZI_IO_FORCE_RENEW);
            KubeResourceManager.get().kubeClient().getClient()
                .secrets().inNamespace(testStorage.getNamespaceName()).withName(secretName).patch(PatchContext.of(PatchType.JSON), annotated);
        }

        if (kafkaShouldRoll) {
            LOGGER.info("Waiting for Kafka rolling restart");
            RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getControllerSelector(), 3, controllerPods);
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
            Secret secret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(secretName).get();
            assertThat("Secret " + secretName + " should exist", secret, is(notNullValue()));
            assertThat("CA cert in " + secretName + " should have non-null 'data'", is(notNullValue()));
            String value = secret.getData().get("ca.crt");
            assertThat("CA cert in " + secretName + " should have changed",
                value, is(not(initialCaCerts.get(secretName))));
        }

        kafkaProducerConsumer.setConsumerGroup(ClientUtils.generateRandomConsumerGroup());
        KubeResourceManager.get().createResourceWithWait(kafkaProducerConsumer.getConsumer().getJob());

        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getMessageCount());

        // Check a new client (signed by new client key) can consume
        String bobUserName = "bob-" + testStorage.getUsername();

        KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), bobUserName, testStorage.getClusterName()).build());

        kafkaProducerConsumer.setConsumerGroup(ClientUtils.generateRandomConsumerGroup());
        kafkaProducerConsumer.setAuthentication(ClientsAuthentication.configureTls(testStorage.getClusterName(), bobUserName));

        KubeResourceManager.get().createResourceWithWait(kafkaProducerConsumer.getConsumer().getJob());
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getMessageCount());

        if (!kafkaShouldRoll) {
            assertThat("Kafka Pods should not roll, but did.", PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector()), is(brokerPods));
            assertThat("Kafka Pods should not roll, but did.", PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector()), is(controllerPods));
        }
        if (!eoShouldRoll) {
            assertThat("EO Pod should not roll, but did.", DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), testStorage.getEoDeploymentName()), is(eoPod));
        }
        if (!keAndCCShouldRoll) {
            assertThat("CC Pod should not roll, but did.", DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName())), is(ccPod));
            assertThat("KE Pod should not roll, but did.", DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), KafkaExporterResources.componentName(testStorage.getClusterName())), is(kePod));
        }
    }

    @TestDoc(
        description = @Desc("Test verifying that annotating the cluster CA key secret with strimzi.io/force-replace triggers automatic replacement of the cluster CA key pair, causing multiple rolling updates including removal of the old cluster CA certificate."),
        steps = {
            @Step(value = "Deploy Kafka cluster with TLS listener, Cruise Control, and KafkaExporter.", expected = "Kafka cluster is deployed."),
            @Step(value = "Create KafkaUser and KafkaTopic, produce and consume messages over TLS.", expected = "Messages are exchanged successfully."),
            @Step(value = "Annotate the cluster CA key secret with strimzi.io/force-replace.", expected = "CA key replacement is triggered."),
            @Step(value = "Wait for multiple rolling updates of all components including an additional roll for old certificate removal.", expected = "All components complete multiple rolling updates."),
            @Step(value = "Verify the CA key has changed and clients can still consume messages.", expected = "New key is in use and messaging works.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    @ParallelNamespaceTest
    @Tag(ROLLING_UPDATE)
    @Tag(CRUISE_CONTROL)
    @Tag("ClusterCaKeys")
    void testAutoReplaceClusterCaKeysTriggeredByAnno() {
        autoReplaceSomeKeysTriggeredByAnno(
                3, // additional third rolling due to the removal of the older cluster CA certificate
                true,
                true,
                true);
    }

    @TestDoc(
        description = @Desc("Test verifying that annotating the clients CA key secret with strimzi.io/force-replace triggers automatic replacement of the clients CA key pair, causing a rolling update of Kafka brokers without rolling Entity Operator, KafkaExporter, or Cruise Control."),
        steps = {
            @Step(value = "Deploy Kafka cluster with TLS listener, Cruise Control, and KafkaExporter.", expected = "Kafka cluster is deployed."),
            @Step(value = "Create KafkaUser and KafkaTopic, produce and consume messages over TLS.", expected = "Messages are exchanged successfully."),
            @Step(value = "Annotate the clients CA key secret with strimzi.io/force-replace.", expected = "CA key replacement is triggered."),
            @Step(value = "Wait for Kafka rolling update; verify Entity Operator, KafkaExporter, and Cruise Control do not roll.", expected = "Only Kafka rolls."),
            @Step(value = "Verify the CA key has changed and clients can still consume messages.", expected = "New key is in use and messaging works.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    @ParallelNamespaceTest
    @Tag(ROLLING_UPDATE)
    @Tag(CRUISE_CONTROL)
    @Tag("ClientsCaKeys")
    void testAutoReplaceClientsCaKeysTriggeredByAnno() {
        autoReplaceSomeKeysTriggeredByAnno(
                1,
                true,
                false,
                false);
    }

    @TestDoc(
        description = @Desc("Test verifying that annotating both cluster and clients CA key secrets with strimzi.io/force-replace triggers automatic replacement of all CA key pairs, causing multiple rolling updates of all components."),
        steps = {
            @Step(value = "Deploy Kafka cluster with TLS listener, Cruise Control, and KafkaExporter.", expected = "Kafka cluster is deployed."),
            @Step(value = "Create KafkaUser and KafkaTopic, produce and consume messages over TLS.", expected = "Messages are exchanged successfully."),
            @Step(value = "Annotate both cluster and clients CA key secrets with strimzi.io/force-replace.", expected = "CA key replacement is triggered for both CAs."),
            @Step(value = "Wait for multiple rolling updates of all components.", expected = "All components complete multiple rolling updates."),
            @Step(value = "Verify both CA keys have changed and clients can still consume messages.", expected = "New keys are in use and messaging works.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    @ParallelNamespaceTest
    @Tag(ROLLING_UPDATE)
    @Tag(CRUISE_CONTROL)
    @Tag("AllCaKeys")
    void testAutoReplaceAllCaKeysTriggeredByAnno() {
        autoReplaceSomeKeysTriggeredByAnno(
                3, // additional third rolling due to the removal of the older cluster CA certificate
                true,
                true,
                true);
    }

    @SuppressWarnings({"checkstyle:MethodLength", "checkstyle:NPathComplexity", "checkstyle:CyclomaticComplexity"})
    void autoReplaceSomeKeysTriggeredByAnno(int expectedRolls,
                                            boolean kafkaShouldRoll,
                                            boolean eoShouldRoll,
                                            boolean keAndCCShouldRoll) {

        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        List<String> secrets;

        // to make it parallel we need decision maker...
        if (KubeResourceManager.get().getTestContext().getTags().contains("ClusterCaKeys")) {
            secrets = Arrays.asList(clusterCaKeySecretName(testStorage.getClusterName()));
        } else if (KubeResourceManager.get().getTestContext().getTags().contains("ClientsCaKeys")) {
            secrets = Arrays.asList(clientsCaKeySecretName(testStorage.getClusterName()));
        } else {
            // AllCaKeys
            secrets = Arrays.asList(clusterCaKeySecretName(testStorage.getClusterName()),
                clientsCaKeySecretName(testStorage.getClusterName()));
        }

        createKafkaCluster(testStorage);

        KubeResourceManager.get().createResourceWithWait(
            KafkaUserTemplates.tlsUser(testStorage).build(),
            KafkaTopicTemplates.topic(testStorage).build()
        );

        final KafkaProducerConsumer kafkaProducerConsumer = new KafkaProducerConsumerBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withMessageCount(testStorage.getMessageCount())
            .withAuthentication(ClientsAuthentication.configureTls(testStorage.getClusterName(), testStorage.getUsername()))
            .build();

        KubeResourceManager.get().createResourceWithWait(
            kafkaProducerConsumer.getProducer().getJob(),
            kafkaProducerConsumer.getConsumer().getJob()
        );

        ClientUtils.waitForClientsSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getProducerName(), testStorage.getMessageCount());

        // Get all pods, and their resource versions
        Map<String, String> controllerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());
        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        Map<String, String> eoPod = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), testStorage.getEoDeploymentName());
        Map<String, String> ccPod = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName()));
        Map<String, String> kePod = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), KafkaExporterResources.componentName(testStorage.getClusterName()));

        LOGGER.info("Triggering CA cert renewal by adding the annotation");
        Map<String, String> initialCaKeys = new HashMap<>();
        for (String secretName : secrets) {
            Secret secret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(secretName).get();
            String value = secret.getData().get("ca.key");
            assertThat("ca.key in " + secretName + " should not be null", value, is(Matchers.notNullValue()));
            initialCaKeys.put(secretName, value);
            Secret annotated = new SecretBuilder(secret)
                .editMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_FORCE_REPLACE, "true")
                .endMetadata()
                .build();
            LOGGER.info("Patching Secret: {}/{} with {}", testStorage.getNamespaceName(), secretName, Annotations.ANNO_STRIMZI_IO_FORCE_REPLACE);
            KubeResourceManager.get().kubeClient().getClient()
                .secrets().inNamespace(testStorage.getNamespaceName()).withName(secretName).patch(PatchContext.of(PatchType.JSON), annotated);
        }

        for (int i = 1; i <= expectedRolls; i++) {
            // In case the first rolling update is finished, shut down cluster operator to see if the rolling update can be finished
            if (i == 2) {
                Pod coPod = KubeResourceManager.get().kubeClient().listPodsByPrefixInName(SetupClusterOperator.getInstance().getOperatorNamespace(), SetupClusterOperator.getInstance().getOperatorDeploymentName()).get(0);
                KubeResourceManager.get().deleteResourceWithoutWait(coPod);
            }

            if (kafkaShouldRoll) {
                LOGGER.info("Waiting for Kafka rolling restart ({})", i);
                controllerPods = i < expectedRolls ?
                    RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getControllerSelector(), controllerPods) :
                    RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getControllerSelector(), 3, controllerPods);

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
            Secret secret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(secretName).get();
            assertThat("Secret " + secretName + " should exist", secret, is(notNullValue()));
            assertThat("CA key in " + secretName + " should have non-null 'data'", secret.getData(), is(notNullValue()));
            String value = secret.getData().get("ca.key");
            assertThat("CA key in " + secretName + " should exist", value, is(notNullValue()));
            assertThat("CA key in " + secretName + " should have changed",
                    value, is(not(initialCaKeys.get(secretName))));
        }

        kafkaProducerConsumer.setConsumerGroup(ClientUtils.generateRandomConsumerGroup());
        KubeResourceManager.get().createResourceWithWait(kafkaProducerConsumer.getConsumer().getJob());
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getMessageCount());

        // Finally check a new client (signed by new client key) can consume

        final String bobUserName = "bobik-" + testStorage.getUsername();

        KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), bobUserName, testStorage.getClusterName()).build());

        kafkaProducerConsumer.setConsumerGroup(ClientUtils.generateRandomConsumerGroup());
        kafkaProducerConsumer.setAuthentication(ClientsAuthentication.configureTls(testStorage.getClusterName(), bobUserName));

        KubeResourceManager.get().createResourceWithWait(kafkaProducerConsumer.getConsumer().getJob());
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getMessageCount());

        if (!kafkaShouldRoll) {
            assertThat("Controller Pods should not roll, but did.", PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector()), is(controllerPods));
            assertThat("Broker Pods should not roll, but did.", PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector()), is(brokerPods));
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

        KubeResourceManager.get().createResourceWithWait(
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
        );

        Kafka kafka = KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
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
                    .addToConfig("default.replication.factor", 3)
                .endKafka()
                .withNewCruiseControl()
                .endCruiseControl()
                .withNewKafkaExporter()
                .endKafkaExporter()
            .endSpec()
            .build();

        KubeResourceManager.get().createResourceWithWait(kafka);
    }

    @TestDoc(
        description = @Desc("Test verifying that a cluster deployed with an already-expired cluster CA certificate automatically triggers certificate renewal, and that messaging works after renewal completes."),
        steps = {
            @Step(value = "Create a Kubernetes secret with an already-expired cluster CA certificate.", expected = "Secret with expired certificate is created."),
            @Step(value = "Deploy Kafka cluster which uses the expired certificate.", expected = "Kafka cluster is deployed."),
            @Step(value = "Create KafkaUser and KafkaTopic, produce and consume messages over TLS.", expected = "Messages are exchanged successfully."),
            @Step(value = "Wait for the expired certificate to be automatically renewed and cluster to stabilize.", expected = "Certificate is renewed and cluster is stable."),
            @Step(value = "Produce and consume messages again after renewal.", expected = "Messages are exchanged successfully with renewed certificates.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    @ParallelNamespaceTest
    @Tag(CRUISE_CONTROL)
    void testAutoRenewCaCertsTriggerByExpiredCertificate() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        // 1. Create the Secrets already, and a certificate that's already expired
        InputStream secretInputStream = getClass().getClassLoader().getResourceAsStream("security-st-certs/expired-cluster-ca.crt");
        String clusterCaCert = ReadWriteUtils.readInputStream(secretInputStream);
        SecretUtils.createSecret(testStorage.getNamespaceName(), clusterCaCertificateSecretName(testStorage.getClusterName()), "ca.crt", clusterCaCert);

        // 2. Now create a cluster
        createKafkaCluster(testStorage);

        KubeResourceManager.get().createResourceWithWait(
            KafkaUserTemplates.tlsUser(testStorage).build(),
            KafkaTopicTemplates.topic(testStorage).build()
        );

        final KafkaProducerConsumer kafkaProducerConsumer = new KafkaProducerConsumerBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withMessageCount(testStorage.getMessageCount())
            .withAuthentication(ClientsAuthentication.configureTls(testStorage.getClusterName(), testStorage.getUsername()))
            .build();

        KubeResourceManager.get().createResourceWithWait(
            kafkaProducerConsumer.getProducer().getJob(),
            kafkaProducerConsumer.getConsumer().getJob()
        );

        ClientUtils.waitForClientsSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getProducerName(), testStorage.getMessageCount());

        // Wait until the certificates have been replaced
        SecretUtils.waitForCertToChange(testStorage.getNamespaceName(), clusterCaCert, clusterCaCertificateSecretName(testStorage.getClusterName()), "ca.crt");

        // Wait until the pods are all up and ready
        KafkaUtils.waitForClusterStability(testStorage.getNamespaceName(), testStorage.getClusterName());

        KubeResourceManager.get().createResourceWithWait(
            kafkaProducerConsumer.getProducer().getJob(),
            kafkaProducerConsumer.getConsumer().getJob()
        );

        ClientUtils.waitForClientsSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getProducerName(), testStorage.getMessageCount());
    }

    @TestDoc(
        description = @Desc("Test verifying that CA certificate renewal is deferred until a configured maintenance time window, and that rolling updates only occur within the window."),
        steps = {
            @Step(value = "Deploy Kafka cluster with a maintenance time window set 15 minutes in the future and short CA validity and renewal days.", expected = "Kafka cluster is deployed with maintenance window configured."),
            @Step(value = "Create KafkaUser and KafkaTopic.", expected = "Resources are created."),
            @Step(value = "Update CA validity and renewal days to trigger renewal.", expected = "CA configuration is updated."),
            @Step(value = "Verify that CA certificate generation remains unchanged and no rolling update occurs outside the maintenance window.", expected = "No renewal or rolling update happens."),
            @Step(value = "Add a new maintenance window starting at the current time.", expected = "Maintenance window is updated to start now."),
            @Step(value = "Wait for rolling update to occur within the maintenance window.", expected = "Kafka rolls within the maintenance window."),
            @Step(value = "Verify CA certificate generations have incremented and KafkaUser certificate has been renewed.", expected = "Certificates are renewed."),
            @Step(value = "Produce and consume messages over TLS with renewed certificates.", expected = "Messages are exchanged successfully.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    @ParallelNamespaceTest
    void testCertRenewalInMaintenanceTimeWindow() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
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

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
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

        KubeResourceManager.get().createResourceWithWait(
            KafkaUserTemplates.tlsUser(testStorage).build(),
            KafkaTopicTemplates.topic(testStorage).build()
        );

        Secret kafkaUserSecret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getUsername()).get();

        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        CertificateAuthority newCAValidity = new CertificateAuthority();
        newCAValidity.setRenewalDays(150);
        newCAValidity.setValidityDays(200);

        KafkaUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), k -> {
            k.getSpec().setClusterCa(newCAValidity);
            k.getSpec().setClientsCa(newCAValidity);
        }
        );

        KafkaUtils.waitForKafkaStatusUpdate(testStorage.getNamespaceName(), testStorage.getClusterName());
        Secret secretCaCluster = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(clusterSecretName).get();
        Secret secretCaClients = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(clientsSecretName).get();
        assertThat("Cluster CA certificate has been renewed outside of maintenanceTimeWindows",
                secretCaCluster.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("0"));
        assertThat("Clients CA certificate has been renewed outside of maintenanceTimeWindows",
                secretCaClients.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("0"));

        assertThat("Rolling update was performed outside of maintenanceTimeWindows", brokerPods, is(PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector())));

        maintenanceWindowCron = "* " + LocalDateTime.now().getMinute() + "-" + windowStopMin + " * * * ? *";
        LOGGER.info("Set maintenanceTimeWindow to start now to save time: {}", maintenanceWindowCron);

        List<String> maintenanceTimeWindows = CrdClients.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getSpec().getMaintenanceTimeWindows();
        maintenanceTimeWindows.add(maintenanceWindowCron);

        KafkaUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafka -> kafka.getSpec().setMaintenanceTimeWindows(maintenanceTimeWindows));

        LOGGER.info("Waiting for rolling update to trigger during maintenanceTimeWindows");
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);

        Secret kafkaUserSecretRolled = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getUsername()).get();
        secretCaCluster = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(clusterSecretName).get();
        secretCaClients = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(clientsSecretName).get();

        assertThat("Cluster CA certificate has not been renewed within maintenanceTimeWindows",
                secretCaCluster.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("1"));
        assertThat("Clients CA certificate has not been renewed within maintenanceTimeWindows",
                secretCaClients.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("1"));
        assertThat("KafkaUser certificate has not been renewed within maintenanceTimeWindows",
                kafkaUserSecret, not(sameInstance(kafkaUserSecretRolled)));

        final KafkaProducerConsumer kafkaProducerConsumer = new KafkaProducerConsumerBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withMessageCount(testStorage.getMessageCount())
            .withAuthentication(ClientsAuthentication.configureTls(testStorage.getClusterName(), testStorage.getUsername()))
            .build();

        KubeResourceManager.get().createResourceWithWait(
            kafkaProducerConsumer.getProducer().getJob(),
            kafkaProducerConsumer.getConsumer().getJob()
        );

        ClientUtils.waitForClientsSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getProducerName(), testStorage.getMessageCount());
    }

    @TestDoc(
        description = @Desc("Test verifying that deleting internal CA certificate secrets triggers automatic regeneration of the CA certificates with new certificate data, followed by a rolling update."),
        steps = {
            @Step(value = "Deploy Kafka cluster and create KafkaUser and KafkaTopic.", expected = "All resources are deployed."),
            @Step(value = "Verify CA certificate secrets exist.", expected = "Secrets are present with certificate data."),
            @Step(value = "Delete all CA certificate secrets.", expected = "Secrets are deleted."),
            @Step(value = "Wait for Kafka rolling update and secret regeneration.", expected = "Pods roll and secrets are recreated."),
            @Step(value = "Verify regenerated certificates have different data than the originals.", expected = "New certificates differ from deleted ones."),
            @Step(value = "Produce and consume messages over TLS with regenerated certificates.", expected = "Messages are exchanged successfully.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    @ParallelNamespaceTest
    void testCertRegeneratedAfterInternalCAisDeleted() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build());

        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        KubeResourceManager.get().createResourceWithWait(
            KafkaUserTemplates.tlsUser(testStorage).build(),
            KafkaTopicTemplates.topic(testStorage).build()
        );

        List<Secret> secrets = KubeResourceManager.get().kubeClient().getClient()
            .secrets().inNamespace(testStorage.getNamespaceName()).list().getItems().stream()
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
            SecretUtils.deleteSecretWithWait(testStorage.getNamespaceName(), s.getMetadata().getName());
        }

        PodUtils.verifyThatRunningPodsAreStable(testStorage.getNamespaceName(), testStorage.getBrokerComponentName());
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);

        for (Secret s : secrets) {
            SecretUtils.waitForSecretReady(testStorage.getNamespaceName(), s.getMetadata().getName(), () -> { });
        }

        List<Secret> regeneratedSecrets = KubeResourceManager.get().kubeClient().getClient()
            .secrets().inNamespace(testStorage.getNamespaceName()).list().getItems()
            .stream()
            .filter(secret -> secret.getMetadata().getName().endsWith("ca-cert"))
            .toList();

        for (int i = 0; i < secrets.size(); i++) {
            assertThat("Certificates has different cert UIDs", !secrets.get(i).getData().get("ca.crt").equals(regeneratedSecrets.get(i).getData().get("ca.crt")));
        }

        final KafkaProducerConsumer kafkaProducerConsumer = new KafkaProducerConsumerBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withMessageCount(testStorage.getMessageCount())
            .withAuthentication(ClientsAuthentication.configureTls(testStorage.getClusterName(), testStorage.getUsername()))
            .build();

        KubeResourceManager.get().createResourceWithWait(
            kafkaProducerConsumer.getProducer().getJob(),
            kafkaProducerConsumer.getConsumer().getJob()
        );

        ClientUtils.waitForClientsSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getProducerName(), testStorage.getMessageCount());
    }

    @TestDoc(
        description = @Desc("Test verifying TLS hostname verification behavior with KafkaConnect, where connecting via IP address fails with default hostname verification and succeeds when ssl.endpoint.identification.algorithm is set to empty."),
        steps = {
            @Step(value = "Deploy Kafka cluster and get the bootstrap service IP address.", expected = "Kafka is deployed and IP is retrieved."),
            @Step(value = "Deploy KafkaConnect configured to connect via IP address without disabling hostname verification.", expected = "KafkaConnect enters CrashLoopBackOff due to hostname verification failure."),
            @Step(value = "Update KafkaConnect configuration to set ssl.endpoint.identification.algorithm to empty.", expected = "KafkaConnect configuration is updated."),
            @Step(value = "Verify KafkaConnect recovers and becomes Ready.", expected = "KafkaConnect is in Ready state.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    @ParallelNamespaceTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testTlsHostnameVerificationWithKafkaConnect() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build());
        LOGGER.info("Getting IP of the bootstrap service");

        String ipOfBootstrapService = KubeResourceManager.get().kubeClient().getClient().services().inNamespace(testStorage.getNamespaceName()).withName(KafkaResources.bootstrapServiceName(testStorage.getClusterName())).get().getSpec().getClusterIP();

        LOGGER.info("KafkaConnect without config {} will not connect to {}:9093", "ssl.endpoint.identification.algorithm", ipOfBootstrapService);

        KubeResourceManager.get().createResourceWithoutWait(KafkaConnectTemplates.kafkaConnect(testStorage.getNamespaceName(), testStorage.getClusterName(), 1)
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

        String kafkaConnectPodName = KubeResourceManager.get().kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector()).get(0).getMetadata().getName();

        PodUtils.waitUntilPodIsInCrashLoopBackOff(testStorage.getNamespaceName(), kafkaConnectPodName);

        assertThat("CrashLoopBackOff", is(KubeResourceManager.get().kubeClient().getClient().pods().inNamespace(testStorage.getNamespaceName()).withName(kafkaConnectPodName).get().getStatus().getContainerStatuses()
                .get(0).getState().getWaiting().getReason()));

        KafkaConnectUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), kc -> {
            kc.getSpec().getConfig().put("ssl.endpoint.identification.algorithm", "");
        });

        LOGGER.info("KafkaConnect with config {} will connect to {}:9093", "ssl.endpoint.identification.algorithm", ipOfBootstrapService);

        KafkaConnectUtils.waitForConnectReady(testStorage.getNamespaceName(), testStorage.getClusterName());
    }

    @TestDoc(
        description = @Desc("Test verifying Kafka ACL authorization with separate read and write users over a NodePort TLS listener, ensuring write-only users cannot consume and read-only users cannot produce."),
        steps = {
            @Step(value = "Deploy Kafka cluster with simple authorization and a NodePort TLS listener.", expected = "Kafka cluster is deployed with ACL authorization."),
            @Step(value = "Create a KafkaUser with write-only ACL permissions and send messages.", expected = "Messages are sent successfully."),
            @Step(value = "Attempt to receive messages as the write-only user.", expected = "Receive fails with GroupAuthorizationException."),
            @Step(value = "Create a KafkaUser with read-only ACL permissions for a specific consumer group.", expected = "Read-only user is created."),
            @Step(value = "Receive messages as the read-only user.", expected = "Messages are received successfully."),
            @Step(value = "Attempt to send messages as the read-only user.", expected = "Send fails with authorization exception.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    @ParallelNamespaceTest
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    void testAclRuleReadAndWrite() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final String kafkaUserWrite = "kafka-user-write";
        final String kafkaUserRead = "kafka-user-read";
        final int numberOfMessages = 500;
        final String consumerGroupName = "consumer-group-name-1";

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
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

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getClusterName()).build());
        KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), kafkaUserWrite, testStorage.getClusterName())
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

        KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), kafkaUserRead, testStorage.getClusterName())
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

    @TestDoc(
        description = @Desc("Test verifying that a Kafka super user can both produce and consume messages regardless of ACL restrictions, while a non-super user with write-only ACLs cannot consume."),
        steps = {
            @Step(value = "Deploy Kafka cluster with simple authorization, a NodePort TLS listener, and a configured super user.", expected = "Kafka cluster is deployed with super user configured."),
            @Step(value = "Create a KafkaUser configured as super user with write-only ACL and send messages.", expected = "Messages are sent successfully."),
            @Step(value = "Receive messages as the super user despite having only write ACL.", expected = "Messages are received successfully due to super user privileges."),
            @Step(value = "Create a non-super user with write-only ACL and send messages.", expected = "Messages are sent successfully."),
            @Step(value = "Attempt to receive messages as the non-super user.", expected = "Receive fails with GroupAuthorizationException.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    @ParallelNamespaceTest
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    void testAclWithSuperUser() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
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

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getClusterName()).build());
        KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), testStorage.getKafkaUsername(), testStorage.getClusterName())
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

        KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), nonSuperuserName, testStorage.getClusterName())
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

    @TestDoc(
        description = @Desc("Test verifying that CA certificate renewal completes successfully even when a broker Pod becomes stuck in Pending state during the rolling update, simulating a break-in-the-middle scenario."),
        steps = {
            @Step(value = "Deploy Kafka cluster with short CA validity and renewal days and replication factor of 3.", expected = "Kafka cluster is deployed with short-lived certificates."),
            @Step(value = "Create KafkaUser and KafkaTopic, produce and consume messages over TLS.", expected = "Messages are exchanged successfully."),
            @Step(value = "Set impossibly high CPU resource requirements on brokers and update CA configuration to trigger renewal.", expected = "Broker Pods enter Pending state during rolling update."),
            @Step(value = "Verify a consumer can still read previously produced messages.", expected = "Consumer reads messages from available replicas."),
            @Step(value = "Fix CPU resource requirements to allow Pods to schedule.", expected = "Pods become schedulable."),
            @Step(value = "Wait for certificate renewal and rolling updates to complete.", expected = "All components roll and certificates are renewed."),
            @Step(value = "Produce and consume messages on a new topic with renewed certificates.", expected = "Messages are exchanged successfully with new certificates.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    @ParallelNamespaceTest
    void testCaRenewalBreakInMiddle() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        // Extra Kafka configuration ensures that topic created automatically (by producer) will have data available on more than just single replica once one of broker fails
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
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

        KubeResourceManager.get().createResourceWithWait(
            KafkaUserTemplates.tlsUser(testStorage).build(),
            KafkaTopicTemplates.topic(testStorage)
                .editOrNewSpec()
                    .withReplicas(3)
                .endSpec()
                .build()
        );

        final KafkaProducerConsumer kafkaProducerConsumer = new KafkaProducerConsumerBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withMessageCount(testStorage.getMessageCount())
            .withAuthentication(ClientsAuthentication.configureTls(testStorage.getClusterName(), testStorage.getUsername()))
            .build();

        KubeResourceManager.get().createResourceWithWait(
            kafkaProducerConsumer.getProducer().getJob(),
            kafkaProducerConsumer.getConsumer().getJob()
        );

        ClientUtils.waitForClientsSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getProducerName(), testStorage.getMessageCount());

        Map<String, String> controllerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());
        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        Map<String, String> eoPods = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), testStorage.getEoDeploymentName());

        final String clusterCaCert = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(clusterCaCertificateSecretName(testStorage.getClusterName())).get().getData().get("ca.crt");

        final ResourceRequirements requirements = new ResourceRequirementsBuilder()
            .addToRequests("cpu", new Quantity("100000m"))
            .build();

        KafkaNodePoolUtils.replace(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), knp -> knp.getSpec().setResources(requirements));
        KafkaUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), k -> k.getSpec().setClusterCa(new CertificateAuthorityBuilder()
            .withRenewalDays(4)
            .withValidityDays(7)
            .build())
        );

        TestUtils.waitFor("any Kafka Pod to be in the pending phase because of selected high cpu resource",
            TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> {
                List<Pod> pendingPods = KubeResourceManager.get().kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getBrokerComponentName())
                    .stream().filter(pod -> pod.getStatus().getPhase().equals("Pending")).toList();
                if (pendingPods.isEmpty()) {
                    LOGGER.info("No Pods of {} are in desired state", testStorage.getBrokerComponentName());
                    return false;
                } else {
                    LOGGER.info("Pod in 'Pending' state: {}", pendingPods.get(0).getMetadata().getName());
                    return true;
                }
            }
        );

        kafkaProducerConsumer.setConsumerGroup(ClientUtils.generateRandomConsumerGroup());
        KubeResourceManager.get().createResourceWithWait(kafkaProducerConsumer.getConsumer().getJob());
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getMessageCount());

        final ResourceRequirements correctRequirements = new ResourceRequirementsBuilder()
            .addToRequests("cpu", new Quantity("200m"))
            .build();

        KafkaNodePoolUtils.replace(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(),
            knp -> knp.getSpec().setResources(correctRequirements));

        // Wait for the certificates to get replaced
        SecretUtils.waitForCertToChange(testStorage.getNamespaceName(), clusterCaCert, KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()), "ca.crt");
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getControllerSelector(), 3, controllerPods);
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);
        DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), testStorage.getEoDeploymentName(), 1, eoPods);

        kafkaProducerConsumer.setConsumerGroup(ClientUtils.generateRandomConsumerGroup());
        KubeResourceManager.get().createResourceWithWait(kafkaProducerConsumer.getConsumer().getJob());
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getMessageCount());

        // Try to send and receive messages with new certificates
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), topicName, testStorage.getClusterName()).build());

        kafkaProducerConsumer.setConsumerGroup(ClientUtils.generateRandomConsumerGroup());
        kafkaProducerConsumer.setTopicName(topicName);

        KubeResourceManager.get().createResourceWithWait(
            kafkaProducerConsumer.getProducer().getJob(),
            kafkaProducerConsumer.getConsumer().getJob()
        );
        ClientUtils.waitForClientsSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getProducerName(), testStorage.getMessageCount());
    }

    @TestDoc(
        description = @Desc("Test verifying that KafkaConnect cannot connect to a Kafka cluster when configured with an incompatible TLS version, and recovers when updated to use a matching TLS version."),
        steps = {
            @Step(value = "Deploy Kafka cluster configured to support only TLSv1.2.", expected = "Kafka cluster is deployed with TLSv1.2."),
            @Step(value = "Deploy KafkaConnect configured with TLSv1.", expected = "KafkaConnect becomes NotReady due to TLS version mismatch."),
            @Step(value = "Update KafkaConnect configuration to use TLSv1.2.", expected = "KafkaConnect configuration is updated."),
            @Step(value = "Verify KafkaConnect becomes Ready and is stable.", expected = "KafkaConnect is Ready and stable.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    @ParallelNamespaceTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testKafkaAndKafkaConnectTlsVersion() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final Map<String, Object> configWithNewestVersionOfTls = new HashMap<>();

        final String tlsVersion12 = "TLSv1.2";
        final String tlsVersion1 = "TLSv1";

        configWithNewestVersionOfTls.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, tlsVersion12);
        configWithNewestVersionOfTls.put(SslConfigs.SSL_PROTOCOL_CONFIG, SslConfigs.DEFAULT_SSL_PROTOCOL);

        LOGGER.info("Deploying Kafka cluster with the support {} TLS",  tlsVersion12);

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withConfig(configWithNewestVersionOfTls)
                .endKafka()
            .endSpec()
            .build());

        Map<String, Object> configsFromKafkaCustomResource = CrdClients.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getSpec().getKafka().getConfig();

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

        KubeResourceManager.get().createResourceWithoutWait(KafkaConnectTemplates.kafkaConnect(testStorage.getNamespaceName(), testStorage.getClusterName(), 1)
            .editSpec()
                .withConfig(configWithLowestVersionOfTls)
            .endSpec()
            .build());

        LOGGER.info("Verifying that KafkaConnect status is NotReady because of different TLS version");

        KafkaConnectUtils.waitForConnectNotReady(testStorage.getNamespaceName(), testStorage.getClusterName());

        LOGGER.info("Replacing KafkaConnect config to the newest(TLSv1.2) one same as the Kafka Broker has");

        KafkaConnectUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafkaConnect -> kafkaConnect.getSpec().setConfig(configWithNewestVersionOfTls));

        LOGGER.info("Verifying that KafkaConnect has the accepted configuration:\n {} -> {}\n {} -> {}",
                SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG,
                tlsVersion12,
                SslConfigs.SSL_PROTOCOL_CONFIG,
                SslConfigs.DEFAULT_SSL_PROTOCOL);

        KafkaConnectUtils.waitForKafkaConnectConfigChange(testStorage.getNamespaceName(), tlsVersion12, SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, testStorage.getClusterName());
        KafkaConnectUtils.waitForKafkaConnectConfigChange(testStorage.getNamespaceName(), SslConfigs.DEFAULT_SSL_PROTOCOL, SslConfigs.SSL_PROTOCOL_CONFIG, testStorage.getClusterName());

        LOGGER.info("Verifying that KafkaConnect is stable");

        PodUtils.verifyThatRunningPodsAreStable(testStorage.getNamespaceName(), KafkaConnectResources.componentName(testStorage.getClusterName()));

        LOGGER.info("Verifying that KafkaConnect status is Ready because of same TLS version");

        KafkaConnectUtils.waitForConnectReady(testStorage.getNamespaceName(), testStorage.getClusterName());
    }

    @TestDoc(
        description = @Desc("Test verifying that KafkaConnect cannot connect to a Kafka cluster when configured with an incompatible cipher suite, and recovers when updated to use a matching cipher suite."),
        steps = {
            @Step(value = "Deploy Kafka cluster configured with TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384 cipher suite.", expected = "Kafka cluster is deployed with specific cipher suite."),
            @Step(value = "Deploy KafkaConnect configured with TLS_DHE_RSA_WITH_AES_128_GCM_SHA256 cipher suite.", expected = "KafkaConnect becomes NotReady due to cipher suite mismatch."),
            @Step(value = "Update KafkaConnect configuration to use the same cipher suite as Kafka.", expected = "KafkaConnect configuration is updated."),
            @Step(value = "Verify KafkaConnect becomes Ready and is stable.", expected = "KafkaConnect is Ready and stable.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    @ParallelNamespaceTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testKafkaAndKafkaConnectCipherSuites() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final Map<String, Object> configWithCipherSuitesSha384 = new HashMap<>();

        final String cipherSuitesSha384 = "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384";
        final String cipherSuitesSha256 = "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256";

        configWithCipherSuitesSha384.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, cipherSuitesSha384);

        LOGGER.info("Deploying Kafka cluster with the support {} cipher algorithms",  cipherSuitesSha384);

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withConfig(configWithCipherSuitesSha384)
                .endKafka()
            .endSpec()
            .build());

        Map<String, Object> configsFromKafkaCustomResource = CrdClients.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getSpec().getKafka().getConfig();

        LOGGER.info("Verifying that KafkaConnect has the accepted configuration:\n {} -> {}",
                SslConfigs.SSL_CIPHER_SUITES_CONFIG, configsFromKafkaCustomResource.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG));

        assertThat(configsFromKafkaCustomResource.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG), is(cipherSuitesSha384));

        Map<String, Object> configWithCipherSuitesSha256 = new HashMap<>();

        configWithCipherSuitesSha256.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, cipherSuitesSha256);

        KubeResourceManager.get().createResourceWithoutWait(KafkaConnectTemplates.kafkaConnect(testStorage.getNamespaceName(), testStorage.getClusterName(), 1)
            .editSpec()
                .withConfig(configWithCipherSuitesSha256)
            .endSpec()
            .build());

        LOGGER.info("Verifying that KafkaConnect status is NotReady because of different cipher suites complexity of algorithm");

        KafkaConnectUtils.waitForConnectNotReady(testStorage.getNamespaceName(), testStorage.getClusterName());

        LOGGER.info("Replacing KafkaConnect config to the cipher suites same as the Kafka Broker has");

        KafkaConnectUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafkaConnect -> kafkaConnect.getSpec().setConfig(configWithCipherSuitesSha384));

        LOGGER.info("Verifying that KafkaConnect has the accepted configuration:\n {} -> {}",
                SslConfigs.SSL_CIPHER_SUITES_CONFIG, configsFromKafkaCustomResource.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG));

        KafkaConnectUtils.waitForKafkaConnectConfigChange(testStorage.getNamespaceName(), cipherSuitesSha384, SslConfigs.SSL_CIPHER_SUITES_CONFIG, testStorage.getClusterName());

        LOGGER.info("Verifying that KafkaConnect is stable");

        PodUtils.verifyThatRunningPodsAreStable(testStorage.getNamespaceName(), KafkaConnectResources.componentName(testStorage.getClusterName()));

        LOGGER.info("Verifying that KafkaConnect status is Ready because of the same cipher suites complexity of algorithm");

        KafkaConnectUtils.waitForConnectReady(testStorage.getNamespaceName(), testStorage.getClusterName());
    }

    @TestDoc(
        description = @Desc("Test verifying that CA secrets with generateSecretOwnerReference set to false persist after Kafka deletion, and CA secrets with generateSecretOwnerReference set to true are automatically deleted with the Kafka cluster."),
        steps = {
            @Step(value = "Deploy Kafka cluster with generateSecretOwnerReference set to false for both cluster and clients CAs.", expected = "Kafka cluster is deployed."),
            @Step(value = "Delete the Kafka cluster.", expected = "Kafka cluster is deleted."),
            @Step(value = "Verify that CA secrets still exist after Kafka deletion.", expected = "CA secrets persist."),
            @Step(value = "Delete the persisted CA secrets manually.", expected = "Secrets are deleted."),
            @Step(value = "Deploy a new Kafka cluster with generateSecretOwnerReference set to true.", expected = "New Kafka cluster is deployed."),
            @Step(value = "Delete the Kafka cluster.", expected = "Kafka cluster is deleted."),
            @Step(value = "Verify that CA secrets are automatically deleted with the Kafka cluster.", expected = "CA secrets are deleted.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    @ParallelNamespaceTest
    void testOwnerReferenceOfCASecrets() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        /* Different name for Kafka cluster to make the test quicker -> KafkaRoller is waiting for pods of "my-cluster" to become ready
         for 5 minutes -> this will prevent the waiting. */
        final String secondClusterName = "my-second-cluster-" + testStorage.getClusterName();

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
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
        List<Secret> caSecrets = KubeResourceManager.get().kubeClient().getClient()
            .secrets().inNamespace(testStorage.getNamespaceName()).list().getItems().stream()
            .filter(secret -> secret.getMetadata().getName().contains(KafkaResources.clusterCaKeySecretName(testStorage.getClusterName())) || secret.getMetadata().getName().contains(KafkaResources.clientsCaKeySecretName(testStorage.getClusterName()))).collect(Collectors.toList());

        LOGGER.info("Deleting Kafka: {}/{}", testStorage.getNamespaceName(), testStorage.getClusterName());
        CrdClients.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
        KafkaNodePoolUtils.deleteKafkaNodePoolWithPodSetAndWait(testStorage.getNamespaceName(), testStorage.getClusterName(), testStorage.getBrokerPoolName());
        KafkaNodePoolUtils.deleteKafkaNodePoolWithPodSetAndWait(testStorage.getNamespaceName(), testStorage.getClusterName(), testStorage.getControllerPoolName());
        KafkaUtils.waitForKafkaDeletion(testStorage.getNamespaceName(), testStorage.getClusterName());

        LOGGER.info("Checking actual Secrets after Kafka deletion");
        caSecrets.forEach(caSecret -> {
            String secretName = caSecret.getMetadata().getName();
            LOGGER.info("Checking that Secret: {} is still present", secretName);
            assertNotNull(KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(secretName).get());

            LOGGER.info("Deleting Secret: {}/{}", testStorage.getNamespaceName(), secretName);
            SecretUtils.deleteSecretWithWait(testStorage.getNamespaceName(), secretName);
        });

        LOGGER.info("Deploying Kafka with generateSecretOwnerReference set to true");
        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), secondClusterName, 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), secondClusterName, 3).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), secondClusterName, 3)
            .editOrNewSpec()
                .editOrNewClusterCa()
                    .withGenerateSecretOwnerReference(true)
                .endClusterCa()
                .editOrNewClientsCa()
                    .withGenerateSecretOwnerReference(true)
                .endClientsCa()
            .endSpec()
            .build());

        caSecrets = KubeResourceManager.get().kubeClient().getClient()
            .secrets().inNamespace(testStorage.getNamespaceName()).list().getItems().stream()
            .filter(secret -> secret.getMetadata().getName().contains(KafkaResources.clusterCaKeySecretName(secondClusterName)) || secret.getMetadata().getName().contains(KafkaResources.clientsCaKeySecretName(secondClusterName))).toList();

        LOGGER.info("Deleting Kafka: {}", secondClusterName);
        CrdClients.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(secondClusterName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
        KafkaUtils.waitForKafkaDeletion(testStorage.getNamespaceName(), secondClusterName);

        LOGGER.info("Checking actual Secrets after Kafka deletion");
        caSecrets.forEach(caSecret -> {
            String secretName = caSecret.getMetadata().getName();
            LOGGER.info("Checking that Secret: {} is deleted", secretName);
            TestUtils.waitFor("deletion of Secret: " + secretName, TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
                () -> KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(secretName).get() == null);
        });
    }

    @TestDoc(
        description = @Desc("Test verifying that changing cluster CA validity and renewal days triggers certificate renewal via a rolling update, resulting in updated certificate dates for both the CA and broker certificates."),
        steps = {
            @Step(value = "Deploy Kafka cluster with short cluster CA validity and renewal periods.", expected = "Kafka cluster is deployed."),
            @Step(value = "Record initial CA and broker certificate start and end dates.", expected = "Certificate dates are captured."),
            @Step(value = "Update cluster CA validity to 200 days and renewal to 150 days.", expected = "CA configuration is updated."),
            @Step(value = "Wait for rolling updates of controllers, brokers, and Entity Operator.", expected = "All components roll."),
            @Step(value = "Verify CA and broker certificate end dates have been extended.", expected = "Certificate dates are renewed with longer validity.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    @ParallelNamespaceTest
    void testClusterCACertRenew() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editOrNewSpec()
                .withNewClusterCa()
                    .withRenewalDays(15)
                    .withValidityDays(20)
                .endClusterCa()
            .endSpec()
            .build());

        String brokerPodName = KubeResourceManager.get().kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector()).get(0).getMetadata().getName();
        final Map<String, String> controllerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());
        final Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        final Map<String, String> eoPod = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), testStorage.getEoDeploymentName());

        Secret clusterCASecret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(clusterCaCertificateSecretName(testStorage.getClusterName())).get();
        X509Certificate cacert = SecretUtils.getCertificateFromSecret(clusterCASecret, "ca.crt");
        Date initialCertStartTime = cacert.getNotBefore();
        Date initialCertEndTime = cacert.getNotAfter();

        // Check Broker kafka certificate dates
        Secret brokerCertCreationSecret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(brokerPodName).get();
        X509Certificate kafkaBrokerCert = SecretUtils.getCertificateFromSecret(brokerCertCreationSecret,
            brokerPodName + ".crt");
        Date initialKafkaBrokerCertStartTime = kafkaBrokerCert.getNotBefore();
        Date initialKafkaBrokerCertEndTime = kafkaBrokerCert.getNotAfter();

        LOGGER.info("Change of Kafka validity and renewal days - reconciliation should start");
        CertificateAuthority newClusterCA = new CertificateAuthority();
        newClusterCA.setRenewalDays(150);
        newClusterCA.setValidityDays(200);

        KafkaUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), k -> k.getSpec().setClusterCa(newClusterCA));

        // On the next reconciliation, the Cluster Operator performs a `rolling update`:
        //   a) Controller pods
        //   b) Broker pods
        //   c) and other components to trust the new Cluster CA certificate. (i.e., Entity Operator)
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getControllerSelector(), 3, controllerPods);
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);
        DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), testStorage.getEoDeploymentName(), 1, eoPod);

        // Read renewed secret/certs again
        clusterCASecret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(clusterCaCertificateSecretName(testStorage.getClusterName())).get();
        cacert = SecretUtils.getCertificateFromSecret(clusterCASecret, "ca.crt");
        Date changedCertStartTime = cacert.getNotBefore();
        Date changedCertEndTime = cacert.getNotAfter();

        // Check renewed Broker kafka certificate dates
        brokerCertCreationSecret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(brokerPodName).get();
        kafkaBrokerCert = SecretUtils.getCertificateFromSecret(brokerCertCreationSecret, brokerPodName + ".crt");
        Date changedKafkaBrokerCertStartTime = kafkaBrokerCert.getNotBefore();
        Date changedKafkaBrokerCertEndTime = kafkaBrokerCert.getNotAfter();

        LOGGER.info("Initial ClusterCA cert dates: {} --> {}", initialCertStartTime, initialCertEndTime);
        LOGGER.info("Changed ClusterCA cert dates: {} --> {}", changedCertStartTime, changedCertEndTime);
        LOGGER.info("Kafka Broker cert creation dates: {} --> {}", initialKafkaBrokerCertStartTime, initialKafkaBrokerCertEndTime);
        LOGGER.info("Kafka Broker cert changed dates:  {} --> {}", changedKafkaBrokerCertStartTime, changedKafkaBrokerCertEndTime);

        String msg = "Error: original cert-end date: '" + initialCertEndTime +
                "' ends sooner than changed (prolonged) cert date '" + changedCertEndTime + "'!";
        assertThat(msg, initialCertEndTime.compareTo(changedCertEndTime) < 0);
        assertThat("Broker certificates start dates have not been renewed.",
                initialKafkaBrokerCertStartTime.compareTo(changedKafkaBrokerCertStartTime) < 0);
        assertThat("Broker certificates end dates have not been renewed.",
                initialKafkaBrokerCertEndTime.compareTo(changedKafkaBrokerCertEndTime) < 0);
    }

    @TestDoc(
        description = @Desc("Test verifying that changing clients CA validity and renewal days triggers certificate renewal via a rolling update, resulting in updated certificate dates for both the CA and KafkaUser certificates."),
        steps = {
            @Step(value = "Deploy Kafka cluster with short clients CA validity and renewal periods.", expected = "Kafka cluster is deployed."),
            @Step(value = "Create a KafkaUser with TLS authentication.", expected = "KafkaUser is created."),
            @Step(value = "Record initial CA and user certificate start and end dates.", expected = "Certificate dates are captured."),
            @Step(value = "Update clients CA validity to 200 days and renewal to 150 days.", expected = "CA configuration is updated."),
            @Step(value = "Wait for rolling updates of brokers and Entity Operator.", expected = "Components roll."),
            @Step(value = "Verify CA and user certificate end dates have been extended.", expected = "Certificate dates are renewed with longer validity.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    @ParallelNamespaceTest
    void testClientsCACertRenew() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editOrNewSpec()
                .withNewClientsCa()
                    .withRenewalDays(15)
                    .withValidityDays(20)
                .endClientsCa()
            .endSpec()
            .build());

        String username = "strimzi-tls-user-" + new Random().nextInt(Integer.MAX_VALUE);
        KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), username, testStorage.getClusterName()).build());

        final Map<String, String> controllerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());
        final Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        final Map<String, String> eoPod = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), testStorage.getEoDeploymentName());

        // Check initial clientsCA validity days
        Secret clientsCASecret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(clientsCaCertificateSecretName(testStorage.getClusterName())).get();
        X509Certificate cacert = SecretUtils.getCertificateFromSecret(clientsCASecret, "ca.crt");
        Date initialCertStartTime = cacert.getNotBefore();
        Date initialCertEndTime = cacert.getNotAfter();

        // Check initial kafkauser validity days
        X509Certificate userCert = SecretUtils.getCertificateFromSecret(KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(username).get(), "user.crt");
        Date initialKafkaUserCertStartTime = userCert.getNotBefore();
        Date initialKafkaUserCertEndTime = userCert.getNotAfter();

        LOGGER.info("Change of Kafka validity and renewal days - reconciliation should start");
        CertificateAuthority newClientsCA = new CertificateAuthority();
        newClientsCA.setRenewalDays(150);
        newClientsCA.setValidityDays(200);

        KafkaUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), k -> k.getSpec().setClientsCa(newClientsCA));

        // On the next reconciliation, the Cluster Operator performs a `rolling update`
        // a) controllers have to roll
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, controllerPods);

        // b) brokers have to roll
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);

        // c) EO must roll (because User Operator uses Clients CA for issuing user certificates)
        DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), testStorage.getEoDeploymentName(), 1, eoPod);

        // Read renewed secret/certs again
        clientsCASecret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(clientsCaCertificateSecretName(testStorage.getClusterName())).get();
        cacert = SecretUtils.getCertificateFromSecret(clientsCASecret, "ca.crt");
        Date changedCertStartTime = cacert.getNotBefore();
        Date changedCertEndTime = cacert.getNotAfter();

        userCert = SecretUtils.getCertificateFromSecret(KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(username).get(), "user.crt");
        Date changedKafkaUserCertStartTime = userCert.getNotBefore();
        Date changedKafkaUserCertEndTime = userCert.getNotAfter();

        LOGGER.info("Initial ClientsCA cert dates: {} --> {}", initialCertStartTime, initialCertEndTime);
        LOGGER.info("Changed ClientsCA cert dates: {} --> {}", changedCertStartTime, changedCertEndTime);
        LOGGER.info("Initial userCert dates: {} --> {}", initialKafkaUserCertStartTime, initialKafkaUserCertEndTime);
        LOGGER.info("Changed userCert dates: {} --> {}", changedKafkaUserCertStartTime, changedKafkaUserCertEndTime);


        String msg = "Error: original cert-end date: '" + initialCertEndTime +
                "' ends sooner than changed (prolonged) cert date '" + changedCertEndTime + "'";
        assertThat(msg, initialCertEndTime.compareTo(changedCertEndTime) < 0);
        assertThat("UserCert start date has been renewed",
                initialKafkaUserCertStartTime.compareTo(changedKafkaUserCertStartTime) < 0);
        assertThat("UserCert end date has been renewed",
                initialKafkaUserCertEndTime.compareTo(changedKafkaUserCertEndTime) < 0);
    }

    @TestDoc(
        description = @Desc("Test verifying that broker certificate secrets contain the full CA chain (broker cert + CA cert), with correct subject and issuer fields, and that TLS messaging works with the full chain."),
        steps = {
            @Step(value = "Deploy Kafka cluster.", expected = "Kafka cluster is deployed."),
            @Step(value = "Retrieve the broker certificate chain from the broker secret.", expected = "Certificate chain is retrieved."),
            @Step(value = "Verify the chain contains exactly 2 certificates (broker cert and CA cert) with correct subject and issuer fields.", expected = "Certificate chain is valid."),
            @Step(value = "Create KafkaUser and KafkaTopic, produce and consume messages over TLS.", expected = "Messages are exchanged successfully.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    @ParallelNamespaceTest
    void testBrokerCertificatesIncludeFullCaChain() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        KubeResourceManager.get().createResourceWithWait(
                KafkaNodePoolTemplates.mixedPool(testStorage.getNamespaceName(), testStorage.getMixedPoolName(), testStorage.getClusterName(), 1).build()
        );

        KubeResourceManager.get().createResourceWithWait(
                KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 1).build()
        );

        String brokerPodName = KubeResourceManager.get().kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getMixedSelector()).getFirst().getMetadata().getName();
        LOGGER.info("Getting broker certificate chain from Secret: {}/{}", testStorage.getNamespaceName(), brokerPodName);

        List<X509Certificate> brokerCerts = SecretUtils.getCertificatesFromSecret(
                KubeResourceManager.get().kubeClient().getClient().secrets()
                        .inNamespace(testStorage.getNamespaceName())
                        .withName(brokerPodName)
                        .get(),
                brokerPodName + ".crt");

        LOGGER.info("Verifying broker certificate chain: broker cert -> CA cert");

        assertThat("Broker certificate chain should contain 2 certificates (broker cert + Ca cert)", brokerCerts.size(), is(2));

        X509Certificate brokerCert = brokerCerts.get(0);
        X509Certificate caCert = brokerCerts.get(1);

        LOGGER.info("Broker cert subject: {}", brokerCert.getSubjectX500Principal().getName());
        LOGGER.info("Broker cert issuer: {}", brokerCert.getIssuerX500Principal().getName());
        LOGGER.info("CA cert subject: {}", caCert.getSubjectX500Principal().getName());
        LOGGER.info("CA cert issuer: {}", caCert.getIssuerX500Principal().getName());

        assertThat("Broker certificate subject should contain cluster name",
                brokerCert.getSubjectX500Principal().getName(),
                Matchers.containsString("CN=" + testStorage.getClusterName() + "-kafka"));

        assertThat("Broker certificate should be issued by cluster-ca",
                brokerCert.getIssuerX500Principal().getName(),
                Matchers.containsString("CN=cluster-ca"));

        assertThat("CA certificate subject should be cluster-ca",
                caCert.getSubjectX500Principal().getName(),
                Matchers.containsString("CN=cluster-ca"));

        KubeResourceManager.get().createResourceWithWait(
                KafkaUserTemplates.tlsUser(testStorage).build(),
                KafkaTopicTemplates.topic(testStorage).build()
        );

        final KafkaProducerConsumer kafkaProducerConsumer = new KafkaProducerConsumerBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withMessageCount(testStorage.getMessageCount())
            .withAuthentication(ClientsAuthentication.configureTls(testStorage.getClusterName(), testStorage.getUsername()))
            .build();

        KubeResourceManager.get().createResourceWithWait(
            kafkaProducerConsumer.getProducer().getJob(),
            kafkaProducerConsumer.getConsumer().getJob()
        );

        ClientUtils.waitForClientsSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getProducerName(), testStorage.getMessageCount());
    }

    @BeforeAll
    void setup() {
        SetupClusterOperator
            .getInstance()
            .withDefaultConfiguration()
            .install();
    }
}
