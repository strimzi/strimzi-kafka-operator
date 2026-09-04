/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security;

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
import io.strimzi.testclients.clients.kafka.KafkaProducerConsumer;
import io.strimzi.testclients.clients.kafka.KafkaProducerConsumerBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import javax.security.auth.x500.X500Principal;

import java.security.cert.X509Certificate;
import java.util.Map;

import static io.strimzi.systemtest.TestTags.REGRESSION;
import static org.hamcrest.CoreMatchers.is;
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

    private static String certManagerCaCertSubjectDn;

    @SuppressWarnings("checkstyle:MethodLength")
    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test verifying cert-manager CA integration for both cluster and clients CA, " +
            "including KafkaUser certificate issuance and certificate renewal. " +
            "A new Kafka cluster is deployed with clusterCa.type=cert-manager and clientsCa.type=cert-manager. " +
            "cert-manager issues all component and user end-entity certificates. The cluster must come up healthy, " +
            "Secrets and annotations are verified, KafkaUser cert is verified to be issued by cert-manager, " +
            "and a TLS-authenticated producer/consumer must be able to send and receive messages. " +
            "Then validityDays is updated to trigger certificate renewal and the cluster must remain healthy."),
        steps = {
            @Step(value = "Create the CA cert Secret in the test namespace.",
                  expected = "Secret is present in the test namespace."),
            @Step(value = "Deploy Kafka with clusterCa.type=cert-manager and clientsCa.type=cert-manager, generateCertificateAuthority=false.",
                  expected = "Kafka cluster reaches ready state without errors."),
            @Step(value = "Assert cluster CA cert Secret has correct annotations.",
                  expected = "ca-cert-generation=0, ca-key-generation=0, and cert-hash annotations are set."),
            @Step(value = "Assert the cert-manager broker and cluster operator Secrets (-cm suffix) exist and their certificates match the corresponding Strimzi Secrets and are signed by the cert-manager CA.",
                  expected = "cert-manager Secrets exist, their certificates match the Strimzi Secrets, and the issuer DNs match the CA subject DN."),
            @Step(value = "Create a KafkaUser and assert that the cert-manager managed user Secret (-cm suffix) exists, its tls.crt matches user.crt, and the user cert is signed by the cert-manager CA.",
                  expected = "cert-manager user Secret exists, certificates match, and issuer DN matches cert-manager CA subject DN."),
            @Step(value = "Produce and consume messages over TLS using the KafkaUser.",
                  expected = "Messages are successfully produced and consumed."),
            @Step(value = "Edit the Kafka CR to increase validityDays on clusterCa, causing cert-manager to re-issue broker certs with the new duration.",
                  expected = "Kafka CR is accepted by the API server."),
            @Step(value = "Wait for all broker pods to roll and become ready.",
                  expected = "All broker pods have a new UID after the rolling update."),
            @Step(value = "Produce and consume messages over TLS using a KafkaUser after renewal.",
                  expected = "Messages are successfully produced and consumed.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    void testCertManagerClusterAndClientsCa() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        createCaCertSecret(testStorage.getNamespaceName());

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(
                testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(
                testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
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
                    .withNewClientsCa()
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
                    .endClientsCa()
                .endSpec()
                .build()
        );

        LOGGER.info("Kafka cluster {}/{} is ready with cert-manager Cluster and Clients CA",
            testStorage.getNamespaceName(), testStorage.getClusterName());

        // Assert cluster CA cert Secret has the expected annotations
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

        // Assert the cert-manager managed Secret (-cm suffix) exists and its certificate matches the Strimzi broker Secret
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

        // Assert the cert-manager managed cluster operator Secret (-cm suffix) exists and its certificate matches the Strimzi CO Secret
        final String coSecretName = KafkaResources.clusterOperatorCertsSecretName(testStorage.getClusterName());
        final String certManagerCoSecretName = "cluster-operator-cm";

        final Secret certManagerCoSecret = KubeResourceManager.get().kubeClient().getClient()
            .secrets()
            .inNamespace(testStorage.getNamespaceName())
            .withName(certManagerCoSecretName)
            .get();

        assertThat("cert-manager CO Secret '" + certManagerCoSecretName + "' must exist", certManagerCoSecret, notNullValue());

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

        LOGGER.info("cert-manager CO Secret '{}' certificate matches Strimzi CO Secret '{}'", certManagerCoSecretName, coSecretName);

        // Assert KafkaUser cert is issued by cert-manager
        KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage).build());

        LOGGER.info("KafkaUser {}/{} is ready — asserting cert-manager certificate issuance",
            testStorage.getNamespaceName(), testStorage.getUsername());

        final String certManagerUserSecretName = testStorage.getUsername() + "-cm";
        final Secret certManagerUserSecret = KubeResourceManager.get().kubeClient().getClient()
            .secrets()
            .inNamespace(testStorage.getNamespaceName())
            .withName(certManagerUserSecretName)
            .get();

        assertThat("cert-manager user Secret '" + certManagerUserSecretName + "' must exist", certManagerUserSecret, notNullValue());

        final X509Certificate certManagerUserCert = SecretUtils.getCertificateFromSecret(certManagerUserSecret, "tls.crt");
        assertThat("cert-manager user cert must not be null", certManagerUserCert, notNullValue());

        final Secret userSecret = KubeResourceManager.get().kubeClient().getClient()
            .secrets()
            .inNamespace(testStorage.getNamespaceName())
            .withName(testStorage.getUsername())
            .get();

        assertThat("Strimzi user Secret must exist", userSecret, notNullValue());

        final X509Certificate userCert = SecretUtils.getCertificateFromSecret(userSecret, "user.crt");
        assertThat("user.crt must not be null", userCert, notNullValue());

        assertThat("cert-manager Secret tls.crt must match Strimzi user Secret user.crt",
            certManagerUserCert, is(userCert));

        assertThat("user.crt issuer DN must match the cert-manager CA subject DN",
            userCert.getIssuerX500Principal().getName(), is(certManagerCaCertSubjectDn));

        LOGGER.info("cert-manager user Secret '{}' cert matches Strimzi user Secret '{}', issuer '{}' matches cert-manager CA subject '{}'",
            certManagerUserSecretName, testStorage.getUsername(),
            userCert.getIssuerX500Principal().getName(), certManagerCaCertSubjectDn);

        // Produce and consume messages over TLS
        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());

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

        LOGGER.info("TLS producer/consumer successfully exchanged {} messages", testStorage.getMessageCount());

        LOGGER.info("Verifying cert-manager certificate renewal by updating validityDays");

        final Map<String, String> brokerPodsSnapshot = PodUtils.podSnapshot(
            testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        LOGGER.info("Updating clusterCa validityDays to 730 to trigger cert-manager cert re-issuance");

        CrdClients.kafkaClient()
            .inNamespace(testStorage.getNamespaceName())
            .withName(testStorage.getClusterName())
            .edit(k -> new KafkaBuilder(k)
                .editSpec()
                    .editClusterCa()
                        .withValidityDays(730)
                    .endClusterCa()
                .endSpec()
                .build());

        LOGGER.info("Waiting for broker pods to roll after cert-manager cert re-issuance");
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(
            testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPodsSnapshot);

        LOGGER.info("Broker rolling update completed — verifying cluster is functional");

        KafkaProducerConsumer renewalProducerConsumer =
            new KafkaProducerConsumerBuilder()
                .withProducerName(testStorage.getProducerName() + "-renewal")
                .withConsumerName(testStorage.getConsumerName() + "-renewal")
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
            testStorage.getConsumerName() + "-renewal",
            testStorage.getProducerName() + "-renewal",
            testStorage.getMessageCount()
        );

        LOGGER.info("TLS producer/consumer successfully exchanged {} messages after cert renewal rolling update",
            testStorage.getMessageCount());
    }

    @SuppressWarnings("checkstyle:MethodLength")
    @ParallelNamespaceTest
    @TestDoc(
            description = @Desc("Test verifying migration between all CA types: Strimzi -> cert-manager -> custom -> cert-manager. " +
                "A Kafka cluster is first deployed with the default Strimzi-managed CA, then switched to cert-manager, " +
                "then to a user-provided custom CA, and finally back to cert-manager. " +
                "At each transition the cluster must remain operational and certificates must match the expected CA."),
            steps = {
                @Step(value = "Deploy Kafka with default Strimzi-managed CA.",
                      expected = "Kafka cluster reaches ready state."),
                @Step(value = "Create the cert-manager CA cert Secret and edit the Kafka CR to switch cluster CA to cert-manager.",
                      expected = "Kafka CR is updated."),
                @Step(value = "Wait for broker pods to roll twice (trust new CA, then re-issue certs).",
                      expected = "All broker pods have new UIDs after both rolling updates."),
                @Step(value = "Verify broker certificates are signed by the cert-manager CA.",
                      expected = "Broker certificate issuer DN matches cert-manager CA subject DN."),
                @Step(value = "Produce and consume messages over TLS after switching to cert-manager.",
                      expected = "Messages are successfully produced and consumed."),
                @Step(value = "Pause reconciliation, replace cluster CA secrets with custom CA, edit Kafka CR, resume.",
                      expected = "Kafka CR and secrets are updated atomically."),
                @Step(value = "Wait for broker pods to roll twice (trust new CA, then re-issue certs).",
                      expected = "All broker pods have new UIDs after both rolling updates."),
                @Step(value = "Verify broker certificates are signed by the custom CA.",
                      expected = "Broker certificate issuer DN matches custom CA subject DN."),
                @Step(value = "Produce and consume messages over TLS after switching to custom CA.",
                      expected = "Messages are successfully produced and consumed."),
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
    void testMigrateBetweenCaTypes() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        // Deploy with default Strimzi CA
        KubeResourceManager.get().createResourceWithWait(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(
                        testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(
                        testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );

        KubeResourceManager.get().createResourceWithWait(
                KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build()
        );

        LOGGER.info("Kafka cluster {}/{} is ready with default Strimzi-managed CA",
                testStorage.getNamespaceName(), testStorage.getClusterName());

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());
        KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage).build());

        //Switch from Strimzi to cert-manager
        createCaCertSecret(testStorage.getNamespaceName());

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

        brokerPodsSnapshot = PodUtils.podSnapshot(
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

        brokerPodName = KubeResourceManager.get().kubeClient()
                .listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector())
                .getFirst().getMetadata().getName();

        brokerCertSecret = KubeResourceManager.get().kubeClient().getClient()
                .secrets()
                .inNamespace(testStorage.getNamespaceName())
                .withName(brokerPodName)
                .get();
        assertThat("Broker cert Secret must exist after switching to custom CA", brokerCertSecret, notNullValue());

        brokerCert = SecretUtils.getCertificateFromSecret(brokerCertSecret, brokerPodName + ".crt");
        assertThat("Broker cert must not be null after switching to custom CA", brokerCert, notNullValue());
        assertThat("Broker cert must be signed by custom CA after switching",
                brokerCert.getIssuerX500Principal(), is(new X500Principal(customClusterCa.getSubjectDn())));

        LOGGER.info("Verified broker cert is signed by custom CA (issuer '{}')",
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

        KafkaProducerConsumer cmProducerConsumer =
                new KafkaProducerConsumerBuilder()
                        .withProducerName(testStorage.getProducerName() + "-cm")
                        .withConsumerName(testStorage.getConsumerName() + "-cm")
                        .withNamespaceName(testStorage.getNamespaceName())
                        .withTopicName(testStorage.getTopicName())
                        .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
                        .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
                        .withMessageCount(testStorage.getMessageCount())
                        .withAuthentication(ClientsAuthentication.configureTls(testStorage.getClusterName(), testStorage.getUsername()))
                        .build();

        KubeResourceManager.get().createResourceWithWait(
                cmProducerConsumer.getProducer().getJob(),
                cmProducerConsumer.getConsumer().getJob()
        );

        ClientUtils.waitForClientsSuccess(
                testStorage.getNamespaceName(),
                testStorage.getConsumerName() + "-cm",
                testStorage.getProducerName() + "-cm",
                testStorage.getMessageCount()
        );

        LOGGER.info("TLS producer/consumer successfully exchanged {} messages after switching back to cert-manager CA",
                testStorage.getMessageCount());
    }

    @BeforeAll
    void setup() {
        SetupCertManager.deployCertManager();
        certManagerCaCertSubjectDn = SetupCertManager.createIssuerAndCaSecret();

        SetupClusterOperator.getInstance().install();
    }

    /**
     * Creates the user-provided CA cert Secret in the given namespace
     * that will be referenced in {@code certManager.caCert.secretName}.
     *
     * <p>The public cert value is retrieved from the Secret in the cert-manager namespace
     * that is used for ClusterIssuer to sign end-entity certificates.
     */
    private static void createCaCertSecret(String namespace) {
        final Secret secret = new SecretBuilder()
                .withNewMetadata()
                .withName(CA_CERT_SECRET_NAME)
                .withNamespace(namespace)
                .endMetadata()
                .addToData(CA_CERT_KEY, SetupCertManager.getCaCertBase64())
                .build();

        KubeResourceManager.get().createResourceWithWait(secret);
        LOGGER.info("Created user-provided CA cert Secret '{}/{}'", namespace, CA_CERT_SECRET_NAME);
    }
}
