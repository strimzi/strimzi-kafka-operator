/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security;

import io.fabric8.certmanager.api.model.v1.Certificate;
import io.fabric8.certmanager.api.model.v1.CertificateBuilder;
import io.fabric8.certmanager.api.model.v1.CertificateList;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.common.CertificateManagerType;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.certmanager.IssuerKind;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.ca.Ca;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.kafkaclients.ClientsAuthentication;
import io.strimzi.systemtest.resources.CrdClients;
import io.strimzi.systemtest.resources.certManager.SetupCertManager;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.testclients.clients.kafka.KafkaProducerConsumer;
import io.strimzi.testclients.clients.kafka.KafkaProducerConsumerBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.io.ByteArrayInputStream;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.TestTags.REGRESSION;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * System tests for cert-manager CA integration.
 *
 * <p>The test simulates user setting up cert-manager and providing a Secret with CA public certificate
 * before deploying a Kafka cluster with {@code clusterCa.type: cert-manager}:</p>
 * <ol>
 *   <li>A self-signed CA certificate and private key are generated and stored in a
 *       Kubernetes {@code Secret} in the cert-manager namespace.</li>
 *   <li>A {@code ClusterIssuer} ({@value SetupCertManager#CLUSTER_ISSUER_NAME}) is created,
 *       references the Secret in the cert-manager namespace so that cert-manager uses the CA key
 *       to sign end-entity certificates.</li>
 *   <li>The CA public cert is copied into the test namespace as a separate Secret,
 *       which Strimzi Cluster Operator reads via {@code certManager.caCert} to establish trust.</li>
 * </ol>
 */
@Tag(REGRESSION)
@SuiteDoc(
    description = @Desc("Test suite verifying cert-manager CA integration: The operators delegates issuing of end-entity certificate to an external cert-manager issuer while the CA public cert is provided by the user in a Kubernetes Secret."),
    labels = {
        @Label(value = TestDocsLabels.SECURITY)
    }
)
public class CertManagerST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(CertManagerST.class);

    private static final String CA_CERT_SECRET_NAME = "cert-manager-ca-cert";
    private static final String CA_CERT_KEY = Ca.CA_CRT;

    @SuppressWarnings("checkstyle:MethodLength")
    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test verifying cert-manager CA integration for both cluster and clients CA, " +
            "including KafkaUser certificate issuance and certificate renewal. " +
            "A new Kafka cluster is deployed with clusterCa.type=cert-manager. " +
            "cert-manager issues all component end-entity certificates. The cluster must come up healthy, " +
            "Secrets and annotations are verified." +
            "Then validityDays is updated to trigger certificate renewal and the cluster must remain healthy." +
            "and a TLS-authenticated producer/consumer must be able to send and receive messages."),
        steps = {
            @Step(value = "Create the CA cert Secret in the test namespace.",
                  expected = "Secret is present in the test namespace."),
            @Step(value = "Deploy Kafka with clusterCa.type=cert-manager and generateCertificateAuthority=false.",
                  expected = "Kafka cluster reaches ready state without errors."),
            @Step(value = "Verify that cluster CA cert Secret has correct annotations.",
                  expected = "ca-cert-generation=0, ca-key-generation=0, and cert-hash annotations are set."),
            @Step(value = "Verify that the cert-manager broker and cluster operator Secrets (-cm suffix) exist and their certificates match the corresponding Strimzi Secrets and are signed by the cert-manager CA.",
                  expected = "cert-manager Secrets exist, their certificates match the Strimzi Secrets, and the issuer DNs match the CA subject DN."),
            @Step(value = "Edit the Kafka CR to change validityDays on clusterCa, causing cert-manager to re-issue broker certificates.",
                  expected = "Kafka CR is accepted by the API server."),
            @Step(value = "Wait for all broker pods to roll and become ready.",
                  expected = "All broker pods have a new UID after the rolling update."),
            @Step(value = "Verify that broker certificate is updated.",
                    expected = "Broker certificate does not match the certificate captured before renewal"),
            @Step(value = "Produce and consume messages over TLS using a KafkaUser after renewal.",
                  expected = "Messages are successfully produced and consumed.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    void testCertManagerClusterCa() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        final String certManagerCaCertSubjectDn = createOrUpdateCaCertSecret(testStorage.getNamespaceName());

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(
                testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(
                testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );

        KubeResourceManager.get().createResourceWithWait(
            KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
                .editSpec()
                    .withNewClusterCa()
                        .withGenerateCertificateAuthority(false)
                        .withValidityDays(365)
                        .withType(CertificateManagerType.CERT_MANAGER)
                        .withNewCertManager()
                            .withNewIssuerRef()
                                .withName(SetupCertManager.CLUSTER_ISSUER_NAME)
                                .withKind(IssuerKind.CLUSTER_ISSUER)
                                .withGroup("cert-manager.io")
                            .endIssuerRef()
                            .withNewCaCertRef()
                                .withSecretName(CA_CERT_SECRET_NAME)
                                .withCertificate(CA_CERT_KEY)
                            .endCaCertRef()
                        .endCertManager()
                    .endClusterCa()
                .endSpec()
                .build()
        );

        LOGGER.info("Kafka cluster {}/{} is ready with cert-manager Cluster and Clients CA",
            testStorage.getNamespaceName(), testStorage.getClusterName());

        // Verify that cluster CA cert Secret has the expected annotations
        final Secret clusterCaCertSecret = KubeResourceManager.get().kubeClient().getClient()
            .secrets()
            .inNamespace(testStorage.getNamespaceName())
            .withName(KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()))
            .get();

        assertThat("Cluster CA cert Secret must exist", clusterCaCertSecret, notNullValue());

        final Map<String, String> caCertAnnotations = clusterCaCertSecret.getMetadata().getAnnotations();
        assertThat("ca-cert-generation must be 0 on initial deployment",
            caCertAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("0"));
        assertThat("ca-key-generation must be 0 on initial deployment",
            caCertAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), is("0"));
        assertThat("cert-hash annotation must be present",
            caCertAnnotations.containsKey(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH), is(true));

        LOGGER.info("Cluster CA cert Secret annotations verified: {}", caCertAnnotations);

        // Verify that the cert-manager managed Secret (-cm suffix) exists and its certificate matches the Strimzi broker Secret
        final String brokerPodName = KubeResourceManager.get().kubeClient()
            .listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector())
            .getFirst().getMetadata().getName();

        final String certManagerBrokerSecretName = brokerPodName + "-cm";
        final Secret certManagerBrokerSecret = KubeResourceManager.get().kubeClient().getClient()
            .secrets()
            .inNamespace(testStorage.getNamespaceName())
            .withName(certManagerBrokerSecretName)
            .get();

        assertThat("cert-manager broker Secret '" + certManagerBrokerSecretName + "' must exist", certManagerBrokerSecret, notNullValue());

        final X509Certificate certManagerBrokerCert = SecretUtils.getCertificateFromSecret(certManagerBrokerSecret, "tls.crt");
        assertThat("cert-manager broker cert must not be null", certManagerBrokerCert, notNullValue());

        final Secret brokerCertSecret = KubeResourceManager.get().kubeClient().getClient()
            .secrets()
            .inNamespace(testStorage.getNamespaceName())
            .withName(brokerPodName)
            .get();

        assertThat("Strimzi broker cert Secret '" + brokerPodName + "' must exist", brokerCertSecret, notNullValue());

        final X509Certificate brokerCert = SecretUtils.getCertificateFromSecret(brokerCertSecret, brokerPodName + ".crt");
        assertThat("Strimzi broker cert must not be null", brokerCert, notNullValue());

        assertThat("cert-manager Secret certificate must match the Strimzi broker Secret certificate",
            certManagerBrokerCert, is(brokerCert));

        assertThat("Broker certificate issuer DN must match the cert-manager CA subject DN",
            brokerCert.getIssuerX500Principal().getName(), is(certManagerCaCertSubjectDn));

        LOGGER.info("cert-manager broker Secret '{}' certificate matches Strimzi broker Secret '{}', issuer '{}' matches cert-manager CA subject '{}'",
            certManagerBrokerSecretName, brokerPodName, brokerCert.getIssuerX500Principal().getName(), certManagerCaCertSubjectDn);

        // Verify that the cert-manager managed cluster operator Secret (-cm suffix) exists and its certificate matches the Strimzi CO Secret
        final String coSecretName = KafkaResources.clusterOperatorCertsSecretName(testStorage.getClusterName());

        final Secret certManagerCoSecret = KubeResourceManager.get().kubeClient().getClient()
            .secrets()
            .inNamespace(testStorage.getNamespaceName())
            .withName(coSecretName + "-cm")
            .get();

        assertThat("cert-manager CO Secret '" + coSecretName + "-cm' must exist", certManagerCoSecret, notNullValue());

        final X509Certificate certManagerCoCert = SecretUtils.getCertificateFromSecret(certManagerCoSecret, "tls.crt");
        assertThat("cert-manager CO cert must not be null", certManagerCoCert, notNullValue());

        final Secret coSecret = KubeResourceManager.get().kubeClient().getClient()
            .secrets()
            .inNamespace(testStorage.getNamespaceName())
            .withName(coSecretName)
            .get();

        assertThat("Strimzi CO cert Secret '" + coSecretName + "' must exist", coSecret, notNullValue());

        final X509Certificate coCert = SecretUtils.getCertificateFromSecret(coSecret, "cluster-operator.crt");
        assertThat("Strimzi CO cert must not be null", coCert, notNullValue());

        assertThat("cert-manager CO Secret certificate must match the Strimzi CO Secret certificate",
            certManagerCoCert, is(coCert));

        assertThat("CO certificate issuer DN must match the cert-manager CA subject DN",
            coCert.getIssuerX500Principal().getName(), is(certManagerCaCertSubjectDn));

        LOGGER.info("cert-manager CO Secret '{}' certificate matches Strimzi CO Secret '{}'", coSecretName + "-cm", coSecretName);

        LOGGER.info("Verifying cert-manager certificate renewal by updating validityDays");

        final String brokerCmSecretName = KubeResourceManager.get().kubeClient()
            .listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector())
            .getFirst().getMetadata().getName() + "-cm";
        final String brokerCertBeforeRenewal = KubeResourceManager.get().kubeClient().getClient().secrets()
            .inNamespace(testStorage.getNamespaceName())
            .withName(brokerCmSecretName).get().getData().get("tls.crt");

        final Map<String, String> brokerPodsSnapshot = PodUtils.podSnapshot(
            testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        LOGGER.info("Updating clusterCa validityDays to 30 to trigger cert-manager cert re-issuance");

        CrdClients.kafkaClient()
            .inNamespace(testStorage.getNamespaceName())
            .withName(testStorage.getClusterName())
            .edit(k -> new KafkaBuilder(k)
                .editSpec()
                    .editClusterCa()
                        .withValidityDays(334)
                    .endClusterCa()
                .endSpec()
                .build());

        LOGGER.info("Waiting for broker pods to roll after validityDays change");
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(
            testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPodsSnapshot);

        final String brokerCertAfterRenewal = KubeResourceManager.get().kubeClient().getClient().secrets()
            .inNamespace(testStorage.getNamespaceName())
            .withName(brokerCmSecretName).get().getData().get("tls.crt");
        assertThat("End-entity certificate must be re-issued after validityDays change",
            brokerCertAfterRenewal, is(not(brokerCertBeforeRenewal)));

        LOGGER.info("Broker rolling update completed — end-entity cert was re-issued, verifying cluster is functional");

        // Produce and consume messages over TLS
        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());
        KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage).build());

        KafkaProducerConsumer renewalProducerConsumer =
            new KafkaProducerConsumerBuilder()
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
            renewalProducerConsumer.getProducer().getJob(),
            renewalProducerConsumer.getConsumer().getJob()
        );

        ClientUtils.waitForClientsSuccess(
            testStorage.getNamespaceName(),
            testStorage.getConsumerName(),
            testStorage.getProducerName(),
            testStorage.getMessageCount()
        );

        LOGGER.info("TLS producer/consumer successfully exchanged {} messages after cert renewal rolling update",
            testStorage.getMessageCount());
    }

    @SuppressWarnings("checkstyle:MethodLength")
    @ParallelNamespaceTest
    @TestDoc(
            description = @Desc("Test verifying switch from Strimzi-managed CA to cert-manager CA. " +
                "A Kafka cluster is first deployed with the default Strimzi-managed CA, then switched to " +
                "cert-manager by editing the Kafka CR. The cluster must remain operational " +
                "and certificates must match the expected CA."),
            steps = {
                @Step(value = "Deploy Kafka with default Strimzi-managed CA.",
                      expected = "Kafka cluster reaches ready state."),
                @Step(value = "Create the cert-manager CA cert Secret and edit the Kafka CR to switch cluster CA to cert-manager.",
                      expected = "Kafka CR is updated."),
                @Step(value = "Wait for broker pods to roll twice (trust new CA, then re-issue certs).",
                      expected = "All broker pods have new UIDs after both rolling updates."),
                @Step(value = "Wait for CO cert to be reissued with the new cert-manager CA.",
                      expected = "CO cert secret generation matches cluster CA cert generation."),
                @Step(value = "Verify broker certificates are signed by the cert-manager CA.",
                      expected = "Broker certificate issuer DN matches cert-manager CA subject DN."),
                @Step(value = "Produce and consume messages over TLS after switching to cert-manager.",
                      expected = "Messages are successfully produced and consumed.")
            },
            labels = {
                @Label(value = TestDocsLabels.SECURITY)
            }
    )
    void testSwitchFromStrimziToCertManager() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        // Deploy with default Strimzi CA
        KubeResourceManager.get().createResourceWithWait(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(
                        testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(
                        testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );

        KubeResourceManager.get().createResourceWithWait(
                KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build()
        );

        LOGGER.info("Kafka cluster {}/{} is ready with default Strimzi-managed CA",
                testStorage.getNamespaceName(), testStorage.getClusterName());

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());
        KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage).build());

        //Switch from Strimzi to cert-manager
        final String certManagerCaCertSubjectDn = createOrUpdateCaCertSecret(testStorage.getNamespaceName());

        Map<String, String> brokerPodsSnapshot = PodUtils.podSnapshot(
                testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        LOGGER.info("Editing Kafka CR to switch cluster CA from Strimzi-managed to cert-manager");

        CrdClients.kafkaClient()
                .inNamespace(testStorage.getNamespaceName())
                .withName(testStorage.getClusterName())
                .edit(k -> new KafkaBuilder(k)
                        .editSpec()
                            .withNewClusterCa()
                                .withGenerateCertificateAuthority(false)
                                .withType(CertificateManagerType.CERT_MANAGER)
                                .withNewCertManager()
                                    .withNewIssuerRef()
                                        .withName(SetupCertManager.CLUSTER_ISSUER_NAME)
                                        .withKind(IssuerKind.CLUSTER_ISSUER)
                                        .withGroup("cert-manager.io")
                                    .endIssuerRef()
                                    .withNewCaCertRef()
                                        .withSecretName(CA_CERT_SECRET_NAME)
                                        .withCertificate(CA_CERT_KEY)
                                    .endCaCertRef()
                                .endCertManager()
                            .endClusterCa()
                        .endSpec().build());

        LOGGER.info("Waiting for first round of rolling update (trust new cert-manager CA)");
        brokerPodsSnapshot = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(
                testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPodsSnapshot);

        LOGGER.info("Waiting for second round of rolling update (broker certs re-issued by cert-manager)");
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(
                testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPodsSnapshot);

        // Wait for CO cert to be re-issued with the new cert-manager CA
        final String coCertSecretName = KafkaResources.clusterOperatorCertsSecretName(testStorage.getClusterName());
        final String expectedCoCertGen = Annotations.stringAnnotation(
                KubeResourceManager.get().kubeClient().getClient().secrets()
                        .inNamespace(testStorage.getNamespaceName())
                        .withName(KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName())).get(),
                Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "0");

        LOGGER.info("Waiting for CO cert secret generation to match cluster CA cert generation ({})", expectedCoCertGen);
        TestUtils.waitFor("CO cert secret generation to be updated to " + expectedCoCertGen,
                TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
                () -> {
                    Secret coSecret = KubeResourceManager.get().kubeClient().getClient().secrets()
                            .inNamespace(testStorage.getNamespaceName())
                            .withName(coCertSecretName).get();
                    return coSecret != null
                            && expectedCoCertGen.equals(Annotations.stringAnnotation(coSecret, Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, ""));
                });

        String brokerPodName = KubeResourceManager.get().kubeClient()
                .listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector())
                .getFirst().getMetadata().getName();

        Secret brokerCertSecret = KubeResourceManager.get().kubeClient().getClient()
                .secrets()
                .inNamespace(testStorage.getNamespaceName())
                .withName(brokerPodName)
                .get();

        assertThat("Strimzi broker cert Secret must exist", brokerCertSecret, notNullValue());

        X509Certificate brokerCert = SecretUtils.getCertificateFromSecret(brokerCertSecret, brokerPodName + ".crt");
        assertThat("Broker cert must not be null", brokerCert, notNullValue());

        assertThat("Broker certificate issuer DN must match the cert-manager CA subject DN after migration",
                brokerCert.getIssuerX500Principal().getName(), is(certManagerCaCertSubjectDn));

        LOGGER.info("Verified that broker cert is signed by cert-manager CA (issuer '{}')",
                brokerCert.getIssuerX500Principal().getName());

        KafkaProducerConsumer kafkaProducerConsumer =
                new KafkaProducerConsumerBuilder()
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

        ClientUtils.waitForClientsSuccess(
                testStorage.getNamespaceName(),
                testStorage.getConsumerName(),
                testStorage.getProducerName(),
                testStorage.getMessageCount()
        );

        LOGGER.info("TLS producer/consumer successfully exchanged {} messages with cert-manager CA",
                testStorage.getMessageCount());
    }

    @SuppressWarnings("checkstyle:MethodLength")
    @ParallelNamespaceTest
    @TestDoc(
            description = @Desc("Test verifying switch between cert-manager CA and custom CA. " +
                "A Kafka cluster is deployed with cert-manager cluster CA, then switched to a user-provided custom CA " +
                "and finally back to cert-manager. At each transition the cluster must remain operational " +
                "and certificates must match the expected CA."),
            steps = {
                @Step(value = "Deploy Kafka with cert-manager cluster CA.",
                      expected = "Kafka cluster reaches ready state."),
                @Step(value = "Pause reconciliation, replace cluster CA secrets with custom CA, edit Kafka CR, resume.",
                      expected = "Kafka CR and secrets are updated atomically."),
                @Step(value = "Wait for broker pods to roll twice (trust new CA, then re-issue certs).",
                      expected = "All broker pods have new UIDs after both rolling updates."),
                @Step(value = "Verify broker certificates are signed by the custom CA.",
                      expected = "Broker certificate issuer DN matches custom CA subject DN."),
                @Step(value = "Produce and consume messages over TLS after switching to custom CA.",
                      expected = "Messages are successfully produced and consumed."),
                @Step(value = "Wait for CO cert to be reissued with the custom CA.",
                      expected = "CO cert secret generation matches cluster CA cert generation."),
                @Step(value = "Edit the Kafka CR to switch cluster CA back to cert-manager.",
                      expected = "Kafka CR is updated."),
                @Step(value = "Wait for broker pods to roll twice (trust new CA, then re-issue certs).",
                      expected = "All broker pods have new UIDs after both rolling updates."),
                @Step(value = "Verify broker certificates are signed by the cert-manager CA.",
                      expected = "Broker certificate issuer DN matches cert-manager CA subject DN."),
                @Step(value = "Produce and consume messages over TLS after switching back to cert-manager.",
                      expected = "Messages are successfully produced and consumed.")
            },
            labels = {
                @Label(value = TestDocsLabels.SECURITY)
            }
    )
    void testSwitchBetweenCertManagerAndCustomCa() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        // Deploy with cert-manager cluster CA
        final String certManagerCaCertSubjectDn = createOrUpdateCaCertSecret(testStorage.getNamespaceName());

        KubeResourceManager.get().createResourceWithWait(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(
                        testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(
                        testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );

        KubeResourceManager.get().createResourceWithWait(
                KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
                        .editSpec()
                            .withNewClusterCa()
                                .withGenerateCertificateAuthority(false)
                                .withType(CertificateManagerType.CERT_MANAGER)
                                .withNewCertManager()
                                    .withNewIssuerRef()
                                        .withName(SetupCertManager.CLUSTER_ISSUER_NAME)
                                        .withKind(IssuerKind.CLUSTER_ISSUER)
                                        .withGroup("cert-manager.io")
                                    .endIssuerRef()
                                    .withNewCaCertRef()
                                        .withSecretName(CA_CERT_SECRET_NAME)
                                        .withCertificate(CA_CERT_KEY)
                                    .endCaCertRef()
                                .endCertManager()
                            .endClusterCa()
                        .endSpec()
                        .build()
        );

        LOGGER.info("Kafka cluster {}/{} is ready with cert-manager cluster CA",
                testStorage.getNamespaceName(), testStorage.getClusterName());

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());
        KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage).build());

        // Switch from cert-manager to custom CA
        LOGGER.info("Switching cluster CA from cert-manager to custom CA");

        final SystemTestCertBundle customClusterCa = SystemTestCertBundle.forClusterCa(testStorage);

        KafkaUtils.annotateKafka(testStorage.getNamespaceName(), testStorage.getClusterName(),
                Map.of(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true"));

        Secret existingCaCertSecret = KubeResourceManager.get().kubeClient().getClient()
                .secrets()
                .inNamespace(testStorage.getNamespaceName())
                .withName(KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()))
                .get();

        final String oldCaCertName = customClusterCa.retrieveOldCertificateName(existingCaCertSecret, "ca.crt");
        final String oldCaCertValue = existingCaCertSecret.getData().get("ca.crt");

        customClusterCa.createCustomSecretsFromBundles(testStorage.getNamespaceName(), testStorage.getClusterName());

        Secret clusterCaCertSecret = KubeResourceManager.get().kubeClient().getClient()
                .secrets()
                .inNamespace(testStorage.getNamespaceName())
                .withName(KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()))
                .get();
        clusterCaCertSecret.getData().put(oldCaCertName, oldCaCertValue);

        Secret clusterCaKeySecret = KubeResourceManager.get().kubeClient().getClient()
                .secrets()
                .inNamespace(testStorage.getNamespaceName())
                .withName(KafkaResources.clusterCaKeySecretName(testStorage.getClusterName()))
                .get();

        SystemTestCertBundle.patchSecretAndIncreaseGeneration(clusterCaCertSecret, testStorage, Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION);
        SystemTestCertBundle.patchSecretAndIncreaseGeneration(clusterCaKeySecret, testStorage, Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION);

        Map<String, String> brokerPodsSnapshot = PodUtils.podSnapshot(
                testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        CrdClients.kafkaClient()
                .inNamespace(testStorage.getNamespaceName())
                .withName(testStorage.getClusterName())
                .edit(k -> new KafkaBuilder(k)
                        .editSpec()
                            .withNewClusterCa()
                                .withGenerateCertificateAuthority(false)
                            .endClusterCa()
                        .endSpec()
                        .build());

        KafkaUtils.removeAnnotation(testStorage.getNamespaceName(), testStorage.getClusterName(),
                Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION);

        LOGGER.info("Waiting for first round of rolling update (trust new custom CA)");
        brokerPodsSnapshot = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(
                testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPodsSnapshot);

        LOGGER.info("Waiting for second round of rolling update (broker certs re-issued by custom CA)");
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(
                testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPodsSnapshot);

        String brokerPodName = KubeResourceManager.get().kubeClient()
                .listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector())
                .getFirst().getMetadata().getName();

        Secret brokerCertSecret = KubeResourceManager.get().kubeClient().getClient()
                .secrets()
                .inNamespace(testStorage.getNamespaceName())
                .withName(brokerPodName)
                .get();
        assertThat("Broker cert Secret must exist after switching to custom CA", brokerCertSecret, notNullValue());

        X509Certificate brokerCert = SecretUtils.getCertificateFromSecret(brokerCertSecret, brokerPodName + ".crt");
        assertThat("Broker cert must not be null after switching to custom CA", brokerCert, notNullValue());
        assertThat("Broker cert must not be by cert-manager CA after switching",
                brokerCert.getIssuerX500Principal(), not(certManagerCaCertSubjectDn));

        LOGGER.info("Verified broker cert is no longer signed by cert-manager CA (issuer '{}')",
                brokerCert.getIssuerX500Principal().getName());

        KafkaProducerConsumer customCaProducerConsumer =
                new KafkaProducerConsumerBuilder()
                        .withProducerName(testStorage.getProducerName() + "-custom")
                        .withConsumerName(testStorage.getConsumerName() + "-custom")
                        .withNamespaceName(testStorage.getNamespaceName())
                        .withTopicName(testStorage.getTopicName())
                        .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
                        .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
                        .withMessageCount(testStorage.getMessageCount())
                        .withAuthentication(ClientsAuthentication.configureTls(testStorage.getClusterName(), testStorage.getUsername()))
                        .build();

        KubeResourceManager.get().createResourceWithWait(
                customCaProducerConsumer.getProducer().getJob(),
                customCaProducerConsumer.getConsumer().getJob()
        );

        ClientUtils.waitForClientsSuccess(
                testStorage.getNamespaceName(),
                testStorage.getConsumerName() + "-custom",
                testStorage.getProducerName() + "-custom",
                testStorage.getMessageCount()
        );

        LOGGER.info("TLS producer/consumer successfully exchanged {} messages with custom cluster CA",
                testStorage.getMessageCount());

        // Wait for CO cert to be re-issued with the custom CA before switching back.
        // because cert-manager relies on CO cert to initiate either cert renewal or key replacement.
        final String coCertSecretName = KafkaResources.clusterOperatorCertsSecretName(testStorage.getClusterName());
        final String expectedGeneration = Annotations.stringAnnotation(
                KubeResourceManager.get().kubeClient().getClient().secrets()
                        .inNamespace(testStorage.getNamespaceName())
                        .withName(KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName())).get(),
                Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "0");

        LOGGER.info("Waiting for CO cert secret generation to match cluster CA cert generation ({})", expectedGeneration);
        TestUtils.waitFor("CO cert secret generation to be updated to " + expectedGeneration,
                TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
                () -> {
                    Secret coSecret = KubeResourceManager.get().kubeClient().getClient().secrets()
                            .inNamespace(testStorage.getNamespaceName())
                            .withName(coCertSecretName).get();
                    return coSecret != null
                            && expectedGeneration.equals(Annotations.stringAnnotation(coSecret, Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, ""));
                });

        // Switch from custom CA back to cert-manager
        LOGGER.info("Switching cluster CA from custom CA back to cert-manager");

        brokerPodsSnapshot = PodUtils.podSnapshot(
                testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        CrdClients.kafkaClient()
                .inNamespace(testStorage.getNamespaceName())
                .withName(testStorage.getClusterName())
                .edit(k -> new KafkaBuilder(k)
                        .editSpec()
                            .withNewClusterCa()
                                .withGenerateCertificateAuthority(false)
                                .withType(CertificateManagerType.CERT_MANAGER)
                                .withNewCertManager()
                                    .withNewIssuerRef()
                                        .withName(SetupCertManager.CLUSTER_ISSUER_NAME)
                                        .withKind(IssuerKind.CLUSTER_ISSUER)
                                        .withGroup("cert-manager.io")
                                    .endIssuerRef()
                                    .withNewCaCertRef()
                                        .withSecretName(CA_CERT_SECRET_NAME)
                                        .withCertificate(CA_CERT_KEY)
                                    .endCaCertRef()
                                .endCertManager()
                            .endClusterCa()
                        .endSpec().build());

        LOGGER.info("Waiting for first round of rolling update (trust cert-manager CA again)");
        brokerPodsSnapshot = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(
                testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPodsSnapshot);

        LOGGER.info("Waiting for second round of rolling update (broker certs re-issued by cert-manager)");
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(
                testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPodsSnapshot);

        brokerPodName = KubeResourceManager.get().kubeClient()
                .listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector())
                .getFirst().getMetadata().getName();

        brokerCertSecret = KubeResourceManager.get().kubeClient().getClient()
                .secrets()
                .inNamespace(testStorage.getNamespaceName())
                .withName(brokerPodName)
                .get();
        assertThat("Broker cert Secret must exist after switching back to cert-manager", brokerCertSecret, notNullValue());

        brokerCert = SecretUtils.getCertificateFromSecret(brokerCertSecret, brokerPodName + ".crt");
        assertThat("Broker cert must not be null after switching back to cert-manager", brokerCert, notNullValue());
        assertThat("Broker cert must be signed by cert-manager CA after switching back",
                brokerCert.getIssuerX500Principal().getName(), is(certManagerCaCertSubjectDn));

        LOGGER.info("Verified broker cert is signed by cert-manager CA again (issuer '{}')",
                brokerCert.getIssuerX500Principal().getName());

        KafkaProducerConsumer producerConsumer =
                new KafkaProducerConsumerBuilder()
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
                producerConsumer.getProducer().getJob(),
                producerConsumer.getConsumer().getJob()
        );

        ClientUtils.waitForClientsSuccess(
                testStorage.getNamespaceName(),
                testStorage.getConsumerName(),
                testStorage.getProducerName(),
                testStorage.getMessageCount()
        );

        LOGGER.info("TLS producer/consumer successfully exchanged {} messages after switching back to cert-manager CA",
                testStorage.getMessageCount());
    }

    @SuppressWarnings("checkstyle:MethodLength")
    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test verifying CA key replacement when using cert-manager. " +
            "A Kafka cluster is deployed with cert-manager. " +
            "First, the CA key is replaced followed by reissuing end-entity certs by deleting -cm secrets, " +
            "expects 3 rolling restarts with correct generation annotation progression. " +
            "Then, the CA key is replaced without reissuing end-entity certificates," +
            "expects 2 rolling restarts and cluster stays stable. " +
            "Then reissues end-entity certificates, expects another rolling restart."),
        steps = {
            @Step(value = "Deploy Kafka cluster with cert-manager cluster CA.",
                  expected = "Kafka cluster reaches ready state."),
            @Step(value = "Replace the CA key by deleting the CA cert Secret and waiting for cert-manager to regenerate it.",
                  expected = "CA cert Secret is recreated with new key."),
            @Step(value = "Trigger end-entity cert reissue by deleting -cm secrets.",
                  expected = "cert-manager recreates -cm secrets signed by the new CA."),
            @Step(value = "Verify that no rolling restart happens before updating the user-provided CA cert Secret.",
                  expected = "Broker pods remain stable."),
            @Step(value = "Update the user-provided CA cert Secret with the new CA certificate.",
                  expected = "Cluster Operator detects the new CA and initiates rolling restarts."),
            @Step(value = "Wait for 3 rolling restarts and verify generation annotations after each.",
                  expected = "ca-key-generation incremented on pods after the first roll, ca-cert-generation incremented on both pods and secrets after the second roll, old CA cert removed in the third roll."),
            @Step(value = "Verify cluster is functional after key replacement.",
                  expected = "Messages are successfully produced and consumed."),
            @Step(value = "Replace the CA key by deleting the CA cert Secret and waiting for cert-manager to regenerate it.",
                  expected = "CA cert Secret is recreated with new key."),
            @Step(value = "Update the user-provided CA cert Secret with the new CA certificate.",
                  expected = "Cluster Operator detects the new CA and initiates rolling restarts."),
            @Step(value = "Wait for 2 rolling restarts and verify generation annotations after each.",
                  expected = "ca-key-generation incremented on pods after the first roll, ca-cert-generation incremented on only pods but not on secrets after the second roll"),
            @Step(value = "Verify that cluster is healthy and no further restarts happen after key replacement and not reissuing end-entity certificates",
                  expected = "Broker pods remain stable."),
            @Step(value = "Trigger end-entity cert reissue by deleting -cm secrets.",
                  expected = "cert-manager recreates -cm secrets signed by the new CA."),
            @Step(value = "Wait for the final rolling restart and verify generation annotations on Secrets.",
                  expected = "ca-cert-generation incremented on secrets after the final roll."),
            @Step(value = "Verify cluster is functional after reissuing end-entity certificates.",
                  expected = "Messages are successfully produced and consumed.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    void testCertManagerCaKeyReplacement() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final String namespace = testStorage.getNamespaceName();

        // Create user-provided CA cert Secret for Strimzi
        createOrUpdateCaCertSecret(namespace);

        // Deploy Kafka cluster with cert-manager
        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(
                namespace, testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(
                namespace, testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );

        KubeResourceManager.get().createResourceWithWait(
            KafkaTemplates.kafka(namespace, testStorage.getClusterName(), 3)
                .editSpec()
                    .withNewClusterCa()
                        .withGenerateCertificateAuthority(false)
                        .withType(CertificateManagerType.CERT_MANAGER)
                        .withNewCertManager()
                            .withNewIssuerRef()
                                .withName(SetupCertManager.CLUSTER_ISSUER_NAME)
                                .withKind(IssuerKind.CLUSTER_ISSUER)
                                .withGroup("cert-manager.io")
                            .endIssuerRef()
                            .withNewCaCertRef()
                                .withSecretName(CA_CERT_SECRET_NAME)
                                .withCertificate(CA_CERT_KEY)
                            .endCaCertRef()
                        .endCertManager()
                    .endClusterCa()
                .endSpec()
                .build()
        );

        LOGGER.info("Kafka cluster {}/{} is ready with cert-manager CA", namespace, testStorage.getClusterName());

        LOGGER.info("Replacing CA key");

        final String oldCaCertWithOldKey = SetupCertManager.getCaCertBase64();
        Map<String, String> brokerPodsSnapshot = PodUtils.podSnapshot(namespace, testStorage.getBrokerSelector());
        // Delete the CA Secret so cert-manager regenerates it with a new key
        KubeResourceManager.get().kubeClient().getClient()
                .secrets().inNamespace(SetupCertManager.CERT_MANAGER_NAMESPACE)
                .withName(SetupCertManager.CA_SECRET_NAME).delete();

        // Wait for cert-manager to recreate the CA Secret with a new key
        TestUtils.waitFor("CA cert Secret to be recreated with new key",
                TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
                () -> {
                    Secret recreated = KubeResourceManager.get().kubeClient().getClient()
                            .secrets().inNamespace(SetupCertManager.CERT_MANAGER_NAMESPACE)
                            .withName(SetupCertManager.CA_SECRET_NAME).get();
                    return recreated != null && recreated.getData() != null
                            && !oldCaCertWithOldKey.equals(recreated.getData().get("tls.crt"));
                });

        LOGGER.info("CA Secret recreated with new key — triggering end-entity cert reissue by deleting -cm secrets");

        deleteAndWaitForCmSecrets(namespace, testStorage.getClusterName(),
                testStorage.getBrokerSelector(), testStorage.getControllerSelector());

        // No rolling restart should happen until the user-provided CA cert Secret is updated
        LOGGER.info("Verifying no rolling restart before updating user-provided CA cert Secret");
        RollingUpdateUtils.waitForNoRollingUpdate(namespace, testStorage.getBrokerSelector(), brokerPodsSnapshot);

        // Update user-provided CA cert Secret with the new CA cert
        LOGGER.info("Updating user-provided CA cert Secret with new CA certificate");
        createOrUpdateCaCertSecret(namespace);

        // Wait for 3 rolling restarts
        LOGGER.info("Waiting for the first rolling restart (trust update)");
        brokerPodsSnapshot = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(
                namespace, testStorage.getBrokerSelector(), 3, brokerPodsSnapshot);

        // After 1st roll: only key generation is incremented on pod (trust update patches key gen only),
        // cert generation stays 0 because end-entity certs haven't been updated yet
        Pod brokerPod = KubeResourceManager.get().kubeClient()
                .listPods(namespace, testStorage.getBrokerSelector()).getFirst();
        String brokerPodName = brokerPod.getMetadata().getName();
        assertThat("cluster-ca-cert-generation must still be 0 on pod after 1st roll",
            Annotations.stringAnnotation(brokerPod, Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, ""),
            is("0"));
        assertThat("cluster-ca-key-generation must be 1 on pod after 1st roll",
            Annotations.stringAnnotation(brokerPod, Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, ""),
            is("1"));

        Secret brokerSecret = KubeResourceManager.get().kubeClient().getClient()
                .secrets().inNamespace(namespace).withName(brokerPodName).get();
        assertThat("ca-cert-generation on broker Secret must still be 0 after 1st roll",
            brokerSecret.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION), is("0"));

        LOGGER.info("Waiting for second round of rolling restart (cert re-issue)");
        brokerPodsSnapshot = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(
                namespace, testStorage.getBrokerSelector(), 3, brokerPodsSnapshot);

        // After 2nd roll: ca-cert-generation incremented on both pod and broker Secret
        brokerPod = KubeResourceManager.get().kubeClient()
                .listPods(namespace, testStorage.getBrokerSelector()).getFirst();
        assertThat("cluster-ca-cert-generation must be 1 on pod after 2nd roll",
            Annotations.stringAnnotation(brokerPod, Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, ""),
            is("1"));

        brokerSecret = KubeResourceManager.get().kubeClient().getClient()
                .secrets().inNamespace(namespace).withName(brokerPodName).get();
        assertThat("ca-cert-generation must be 1 on broker Secret after 2nd roll",
            brokerSecret.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION),
            is("1"));

        LOGGER.info("Waiting for 3rd rolling restart (old CA certificate removal)");
        brokerPodsSnapshot = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(
                namespace, testStorage.getBrokerSelector(), 3, brokerPodsSnapshot);

        LOGGER.info("Verifying that CO cert to be re-issued with the new CA");
        final String coCertSecretName = KafkaResources.clusterOperatorCertsSecretName(testStorage.getClusterName());
        TestUtils.waitFor("CO cert secret generation to be updated to 1",
                TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
                () -> {
                    Secret coSecret = KubeResourceManager.get().kubeClient().getClient().secrets()
                            .inNamespace(namespace)
                            .withName(coCertSecretName).get();
                    return coSecret != null
                            && Annotations.stringAnnotation(coSecret, Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "").equals("1");
                });

        // Verify cluster is functional after key replacement
        LOGGER.info("Verifying cluster is functional after CA key replacement");
        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());
        KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage).build());

        KafkaProducerConsumer kafkaProducerConsumer =
            new KafkaProducerConsumerBuilder()
                .withProducerName(testStorage.getProducerName())
                .withConsumerName(testStorage.getConsumerName())
                .withNamespaceName(namespace)
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

        ClientUtils.waitForClientsSuccess(
            namespace,
            testStorage.getConsumerName(),
            testStorage.getProducerName(),
            testStorage.getMessageCount()
        );

        LOGGER.info("CA key replacement is complete, after 3 rolls, cluster is functional");

        LOGGER.info("Replacing CA key but not renewing end-entity certificates");

        final String oldCaCertData = SetupCertManager.getCaCertBase64();
        // Delete the CA Secret so cert-manager regenerates it with a new key
        KubeResourceManager.get().kubeClient().getClient()
                .secrets().inNamespace(SetupCertManager.CERT_MANAGER_NAMESPACE)
                .withName(SetupCertManager.CA_SECRET_NAME).delete();

        // Wait for cert-manager to recreate the CA Secret with a new key
        TestUtils.waitFor("CA cert Secret to be recreated with new key",
                TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
                () -> {
                    Secret recreated = KubeResourceManager.get().kubeClient().getClient()
                            .secrets().inNamespace(SetupCertManager.CERT_MANAGER_NAMESPACE)
                            .withName(SetupCertManager.CA_SECRET_NAME).get();
                    return recreated != null && recreated.getData() != null
                            && !oldCaCertData.equals(recreated.getData().get("tls.crt"));
                });

        // Update user-provided CA cert Secret with the new CA cert
        createOrUpdateCaCertSecret(namespace);

        // Wait for 2 rolling restarts
        LOGGER.info("Waiting for the first rolling restart (trust update)");
        brokerPodsSnapshot = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(
                namespace, testStorage.getBrokerSelector(), 3, brokerPodsSnapshot);

        brokerPod = KubeResourceManager.get().kubeClient()
                .listPods(namespace, testStorage.getBrokerSelector()).getFirst();
        brokerPodName = brokerPod.getMetadata().getName();

        // After 1st roll: only key generation is incremented on pod (trust update patches key gen only),
        assertThat("cluster-ca-cert-generation must still be 1 on pod after 1st roll",
                Annotations.stringAnnotation(brokerPod, Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, ""),
                is("1"));
        assertThat("cluster-ca-key-generation must be 2 on pod after 1st roll",
                Annotations.stringAnnotation(brokerPod, Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, ""),
                is("2"));

        LOGGER.info("Waiting for second round of rolling restart (cluster-ca-cert-generation is incremented)");
        brokerPodsSnapshot = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(
                namespace, testStorage.getBrokerSelector(), 3, brokerPodsSnapshot);

        // After 2nd roll: ca-cert-generation incremented on the pod but not on the cert Secret because end-entity certificates are not re-issued yet
        brokerPod = KubeResourceManager.get().kubeClient()
                .listPods(namespace, testStorage.getBrokerSelector()).getFirst();
        assertThat("cluster-ca-cert-generation must be 2 on pod after 2nd roll",
                Annotations.stringAnnotation(brokerPod, Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, ""),
                is("2"));

        brokerSecret = KubeResourceManager.get().kubeClient().getClient()
                .secrets().inNamespace(namespace).withName(brokerPodName).get();
        assertThat("ca-cert-generation must be 1 on broker Secret after 2nd roll",
                brokerSecret.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION),
                is("1"));

        LOGGER.info("Verifying cluster stability after CA key replacement without re-issuing end-entity certificates");
        RollingUpdateUtils.waitForNoRollingUpdate(namespace, testStorage.getBrokerSelector(), brokerPodsSnapshot);

        LOGGER.info("CA key replaced without re-issuing end-entity certificates, 2 rolls, the generations are correct, cluster is stable");

        LOGGER.info("Triggering end-entity cert reissue by deleting -cm secrets");

        deleteAndWaitForCmSecrets(namespace, testStorage.getClusterName(),
                testStorage.getBrokerSelector(), testStorage.getControllerSelector());

        LOGGER.info("Waiting for 3rd rolling restart (cert reissue and old CA certificate removal)");
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(
                namespace, testStorage.getBrokerSelector(), 3, brokerPodsSnapshot);

        brokerSecret = KubeResourceManager.get().kubeClient().getClient()
                .secrets().inNamespace(namespace).withName(brokerPodName).get();
        assertThat("ca-cert-generation must be 2 on broker Secret after 3rd roll",
                brokerSecret.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION),
                is("2"));

        LOGGER.info("Verifying cluster is functional after re-issuing end-entity certificates");
        kafkaProducerConsumer = new KafkaProducerConsumerBuilder()
                .withProducerName(testStorage.getProducerName() + "-after-cert-reissue")
                .withConsumerName(testStorage.getConsumerName() + "-after-cert-reissue")
                .withNamespaceName(namespace)
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

        ClientUtils.waitForClientsSuccess(
                namespace,
                testStorage.getConsumerName() + "-after-cert-reissue",
                testStorage.getProducerName() + "-after-cert-reissue",
                testStorage.getMessageCount()
        );
    }

    /**
     * Deletes all cert-manager managed {@code -cm} Secrets for a Kafka cluster and waits for
     * cert-manager to recreate them. This covers broker pods, cluster operator, and entity operator secrets.
     */
    private void deleteAndWaitForCmSecrets(String namespace, String clusterName,
                                              LabelSelector brokerSelector, LabelSelector controllerSelector) {
        List<String> cmSecretNames = new ArrayList<>();

        for (Pod pod : KubeResourceManager.get().kubeClient().listPods(namespace, brokerSelector)) {
            cmSecretNames.add(pod.getMetadata().getName() + "-cm");
        }
        for (Pod pod : KubeResourceManager.get().kubeClient().listPods(namespace, controllerSelector)) {
            String name = pod.getMetadata().getName() + "-cm";
            if (!cmSecretNames.contains(name)) {
                cmSecretNames.add(name);
            }
        }

        cmSecretNames.add(KafkaResources.clusterOperatorCertsSecretName(clusterName) + "-cm");
        cmSecretNames.add(KafkaResources.entityUserOperatorSecretName(clusterName) + "-cm");
        cmSecretNames.add(KafkaResources.entityTopicOperatorSecretName(clusterName) + "-cm");

        for (String cmSecretName : cmSecretNames) {
            KubeResourceManager.get().kubeClient().getClient()
                    .secrets().inNamespace(namespace).withName(cmSecretName).delete();
            LOGGER.info("Deleted -cm secret '{}'", cmSecretName);
        }

        for (String cmSecretName : cmSecretNames) {
            TestUtils.waitFor("-cm secret '" + cmSecretName + "' to be recreated",
                    TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
                    () -> KubeResourceManager.get().kubeClient().getClient()
                            .secrets().inNamespace(namespace).withName(cmSecretName).get() != null);
        }
    }

    @SuppressWarnings("checkstyle:MethodLength")
    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test verifying CA certificate renewal when using cert-manager. " +
            "A Kafka cluster is deployed with cert-manager. " +
            "CA cert is renewed with the same key and then expects 1 rolling restart, " +
            "ca-cert-generation incremented on pods but not on secrets, ca-key-generation unchanged."),
        steps = {
            @Step(value = "Deploy Kafka cluster with cert-manager cluster CA.",
                    expected = "Kafka cluster reaches ready state."),
            @Step(value = "Trigger cert renewal by updating the CA Certificate resource.",
                    expected = "CA cert Secret is recreated with the same key but a new certificate."),
            @Step(value = "Update the user-provided CA cert Secret with the renewed certificate.",
                    expected = "Single rolling restart occurs."),
            @Step(value = "Verify that ca-cert-generation is incremented on pods and ca-key-generation unchanged.",
                    expected = "Generation annotations match expected values."),
            @Step(value = "Verify cluster is functional after certificate renewal.",
                    expected = "Messages are successfully produced and consumed.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    void testCertManagerCaCertRenewal() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final String namespace = testStorage.getNamespaceName();

        // Create user-provided CA cert Secret for Strimzi
        createOrUpdateCaCertSecret(namespace);

        // Deploy Kafka cluster with cert-manager
        KubeResourceManager.get().createResourceWithWait(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(
                        namespace, testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(
                        namespace, testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );

        KubeResourceManager.get().createResourceWithWait(
                KafkaTemplates.kafka(namespace, testStorage.getClusterName(), 3)
                        .editSpec()
                            .withNewClusterCa()
                                .withGenerateCertificateAuthority(false)
                                .withType(CertificateManagerType.CERT_MANAGER)
                                .withNewCertManager()
                                    .withNewIssuerRef()
                                        .withName(SetupCertManager.CLUSTER_ISSUER_NAME)
                                        .withKind(IssuerKind.CLUSTER_ISSUER)
                                        .withGroup("cert-manager.io")
                                    .endIssuerRef()
                                    .withNewCaCertRef()
                                        .withSecretName(CA_CERT_SECRET_NAME)
                                        .withCertificate(CA_CERT_KEY)
                                    .endCaCertRef()
                                .endCertManager()
                            .endClusterCa()
                        .endSpec()
                        .build()
        );

        LOGGER.info("Kafka cluster {}/{} is ready with cert-manager CA", namespace, testStorage.getClusterName());

        LOGGER.info("Renewing CA certificate with the same key");

        final String oldCaCertData = SetupCertManager.getCaCertBase64();

        // Set rotationPolicy to Never so cert-manager reuses the existing key on renewal,
        // and update the DNS names to trigger re-issuing of the certificate.
        KubeResourceManager.get().kubeClient().getClient()
                .resources(Certificate.class, CertificateList.class)
                .inNamespace(SetupCertManager.CERT_MANAGER_NAMESPACE)
                .withName(SetupCertManager.CA_CERTIFICATE_NAME)
                .edit(c -> new CertificateBuilder(c)
                        .editSpec()
                            .withNewPrivateKey()
                                .withRotationPolicy("Never")
                            .endPrivateKey()
                            .withDnsNames("strimzi-ca.local")
                        .endSpec()
                        .build());

        // Wait for cert-manager to re-issue the CA certificate with the same key
        TestUtils.waitFor("CA cert to be re-issued with same key",
                TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
                () -> {
                    Secret caSecret = KubeResourceManager.get().kubeClient().getClient()
                            .secrets().inNamespace(SetupCertManager.CERT_MANAGER_NAMESPACE)
                            .withName(SetupCertManager.CA_SECRET_NAME).get();
                    return caSecret != null && caSecret.getData() != null
                            && !oldCaCertData.equals(caSecret.getData().get("tls.crt"));
                });

        LOGGER.info("CA certificate renewed by cert-manager (same key) — updating user-provided Secret");

        // Update user-provided CA cert Secret with the renewed cert
        createOrUpdateCaCertSecret(namespace);

        Map<String, String> brokerPodsSnapshot = PodUtils.podSnapshot(namespace, testStorage.getBrokerSelector());
        // Wait for 1 rolling restart
        LOGGER.info("Waiting for single rolling restart after CA cert renewal");
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(
                namespace, testStorage.getBrokerSelector(), 3, brokerPodsSnapshot);

        // ca-cert-generation incremented but not the ca-key-generation on the pod
        Pod brokerPod = KubeResourceManager.get().kubeClient()
                .listPods(namespace, testStorage.getBrokerSelector()).getFirst();
        assertThat("cluster-ca-cert-generation must be 1 on pod after renewing the CA certificate",
                Annotations.stringAnnotation(brokerPod, Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, ""),
                is("1"));
        assertThat("cluster-ca-key-generation must remain 0 after renewing the CA certificate with the same key",
                Annotations.stringAnnotation(brokerPod, Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, ""),
                is("0"));

        LOGGER.info("Verifying cluster is functional after CA cert renewal");
        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());
        KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage).build());

        KafkaProducerConsumer producerConsumer =
                new KafkaProducerConsumerBuilder()
                        .withProducerName(testStorage.getProducerName())
                        .withConsumerName(testStorage.getConsumerName())
                        .withNamespaceName(namespace)
                        .withTopicName(testStorage.getTopicName())
                        .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
                        .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
                        .withMessageCount(testStorage.getMessageCount())
                        .withAuthentication(ClientsAuthentication.configureTls(testStorage.getClusterName(), testStorage.getUsername()))
                        .build();

        KubeResourceManager.get().createResourceWithWait(
                producerConsumer.getProducer().getJob(),
                producerConsumer.getConsumer().getJob()
        );

        ClientUtils.waitForClientsSuccess(
                namespace,
                testStorage.getConsumerName(),
                testStorage.getProducerName(),
                testStorage.getMessageCount()
        );

        LOGGER.info("CA cert renewed (same key), 1 roll, the generations are correct, cluster is functional");
    }

    /**
     * Creates the user-provided CA cert Secret in the given namespace
     * that will be referenced in {@code certManager.caCert.secretName}.
     *
     * <p>The public cert value is retrieved from the Secret in the cert-manager namespace
     * that is used for ClusterIssuer to sign end-entity certificates.
     *
     * @returns subject DN of the CA certificate
     */
    private static String createOrUpdateCaCertSecret(String namespace) {
        String certManagerCaCertBase64 = SetupCertManager.getCaCertBase64();
        final Secret secret = new SecretBuilder()
                .withNewMetadata()
                .withName(CA_CERT_SECRET_NAME)
                .withNamespace(namespace)
                .endMetadata()
                .addToData(CA_CERT_KEY, certManagerCaCertBase64)
                .build();

        KubeResourceManager.get().createOrUpdateResourceWithWait(secret);
        LOGGER.info("Created/updated user-provided CA cert Secret '{}/{}'", namespace, CA_CERT_SECRET_NAME);

        try {
            byte[] certBytes = Base64.getDecoder().decode(certManagerCaCertBase64);
            X509Certificate cert = (X509Certificate) CertificateFactory.getInstance("X.509")
                    .generateCertificate(new ByteArrayInputStream(certBytes));
            return cert.getSubjectX500Principal().getName();
        } catch (Exception e) {
            throw new RuntimeException("Failed to read CA certificate subject DN", e);
        }
    }

    @BeforeAll
    void setup() {
        SetupCertManager.deployCertManager();
        SetupCertManager.createIssuerAndCaSecret();
        SetupClusterOperator.getInstance().install();
    }
}
