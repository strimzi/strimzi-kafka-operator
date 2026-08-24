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
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.testclients.clients.kafka.KafkaProducerConsumer;
import io.strimzi.testclients.clients.kafka.KafkaProducerConsumerBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

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

    @SuppressWarnings("checkstyle:MethodLength")
    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test verifying the cert-manager Cluster CA happy path and certificate renewal. " +
            "A new Kafka cluster is deployed with clusterCa.type=cert-manager. cert-manager issues all component " +
            "end-entity certificates. The cluster must come up healthy, Secrets and annotations are verified, " +
            "and a TLS-authenticated producer/consumer must be able to send and receive messages. " +
            "Then validityDays is updated to trigger certificates renewal. The Cluster Operator detects " +
            "the cert change and rolls the broker pods. After the rolling update the cluster must remain healthy " +
            "and a TLS-authenticated produce/consume must succeed."),
        steps = {
            @Step(value = "Create the CA cert Secret in the test namespace.",
                  expected = "Secret is present in the test namespace."),
            @Step(value = "Deploy Kafka with clusterCa.type=cert-manager, generateCertificateAuthority=false.",
                  expected = "Kafka cluster reaches ready state without errors."),
            @Step(value = "Assert cluster CA cert Secret has correct annotations.",
                  expected = "ca-cert-generation=0, ca-key-generation=0, and cert-hash annotations are set."),
            @Step(value = "Assert the cert-manager broker and cluster operator Secrets (-cm suffix) exist and their certificates match the corresponding Strimzi Secrets and are signed by the cert-manager CA.",
                  expected = "cert-manager Secrets exist, their certificates match the Strimzi Secrets, and the issuer DNs match the CA subject DN."),
            @Step(value = "Produce and consume messages over TLS using a KafkaUser.",
                  expected = "Messages are successfully produced and consumed."),
            @Step(value = "Snapshot broker pod UIDs before the change.",
                  expected = "Snapshot captured."),
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
    void testCertManagerClusterCaAndRenewal() {
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
                .endSpec()
                .build()
        );

        LOGGER.info("Kafka cluster {}/{} is ready with cert-manager Cluster CA",
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

        final String cmSecretName = brokerPodName + "-cm";
        final Secret cmBrokerSecret = KubeResourceManager.get().kubeClient().getClient()
            .secrets()
            .inNamespace(testStorage.getNamespaceName())
            .withName(cmSecretName)
            .get();

        assertThat("cert-manager broker Secret '" + cmSecretName + "' must exist", cmBrokerSecret, notNullValue());

        final X509Certificate cmBrokerCert = SecretUtils.getCertificateFromSecret(cmBrokerSecret, "tls.crt");
        assertThat("cert-manager broker cert must not be null", cmBrokerCert, notNullValue());

        final Secret brokerCertSecret = KubeResourceManager.get().kubeClient().getClient()
            .secrets()
            .inNamespace(testStorage.getNamespaceName())
            .withName(brokerPodName)
            .get();

        assertThat("Strimzi broker cert Secret '" + brokerPodName + "' must exist", brokerCertSecret, notNullValue());

        final X509Certificate brokerCert = SecretUtils.getCertificateFromSecret(brokerCertSecret, brokerPodName + ".crt");
        assertThat("Strimzi broker cert must not be null", brokerCert, notNullValue());

        assertThat("cert-manager Secret certificate must match the Strimzi broker Secret certificate",
            cmBrokerCert, is(brokerCert));

        final X509Certificate caCert = SecretUtils.getCertificateFromSecret(clusterCaCertSecret, Ca.CA_CRT);
        assertThat("Cluster CA cert must not be null", caCert, notNullValue());

        assertThat("Broker certificate issuer DN must match the cert-manager CA subject DN",
            brokerCert.getIssuerX500Principal().getName(),
            is(caCert.getSubjectX500Principal().getName()));

        LOGGER.info("cert-manager Secret '{}' certificate matches Strimzi broker Secret '{}', issuer '{}' matches CA subject '{}'",
            cmSecretName, brokerPodName, brokerCert.getIssuerX500Principal().getName(), caCert.getSubjectX500Principal().getName());

        // Assert the cert-manager managed cluster operator Secret (-cm suffix) exists and its certificate matches the Strimzi CO Secret
        final String coSecretName = KafkaResources.clusterOperatorCertsSecretName(testStorage.getClusterName());
        final String cmCoSecretName = "cluster-operator-cm";

        final Secret cmCoSecret = KubeResourceManager.get().kubeClient().getClient()
            .secrets()
            .inNamespace(testStorage.getNamespaceName())
            .withName(cmCoSecretName)
            .get();

        assertThat("cert-manager CO Secret '" + cmCoSecretName + "' must exist", cmCoSecret, notNullValue());

        final X509Certificate cmCoCert = SecretUtils.getCertificateFromSecret(cmCoSecret, "tls.crt");
        assertThat("cert-manager CO cert must not be null", cmCoCert, notNullValue());

        final Secret coSecret = KubeResourceManager.get().kubeClient().getClient()
            .secrets()
            .inNamespace(testStorage.getNamespaceName())
            .withName(coSecretName)
            .get();

        assertThat("Strimzi CO cert Secret '" + coSecretName + "' must exist", coSecret, notNullValue());

        final X509Certificate coCert = SecretUtils.getCertificateFromSecret(coSecret, "cluster-operator.crt");
        assertThat("Strimzi CO cert must not be null", coCert, notNullValue());

        assertThat("cert-manager CO Secret certificate must match the Strimzi CO Secret certificate",
            cmCoCert, is(coCert));

        assertThat("CO certificate issuer DN must match the cert-manager CA subject DN",
            coCert.getIssuerX500Principal().getName(),
            is(caCert.getSubjectX500Principal().getName()));

        LOGGER.info("cert-manager CO Secret '{}' certificate matches Strimzi CO Secret '{}'", cmCoSecretName, coSecretName);

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());
        KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage).build());

        // Produce and consume messages over TLS
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

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test verifying that when clientsCa.type=cert-manager is configured, the User Operator delegates issuing of KafkaUser TLS certificate to cert-manager. The cert-manager managed Secret (-cm suffix) must exist, and user.crt in the Strimzi user Secret must match its tls.crt."),
        steps = {
            @Step(value = "Create the CA cert Secret in the test namespace and deploy Kafka with both " +
                      "clusterCa.type=cert-manager and clientsCa.type=cert-manager.",
                  expected = "Kafka cluster reaches ready state."),
            @Step(value = "Create a KafkaUser with TLS authentication.",
                  expected = "KafkaUser reaches ready state and its Secret is populated."),
            @Step(value = "Assert that the cert-manager managed user Secret (<username>-cm) exists and " +
                      "its tls.crt matches user.crt in the Strimzi user Secret.",
                  expected = "cert-manager Secret exists and certificates match."),
            @Step(value = "Assert that user.crt is signed by the cert-manager clients CA " +
                      "(issuer DN matches clients CA subject DN).",
                  expected = "User certificate issuer DN matches the clients CA subject DN."),
            @Step(value = "Produce and consume messages over TLS using the KafkaUser.",
                    expected = "Messages are successfully produced and consumed.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    void testKafkaUserCertIssuedByCertManager() {
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

        KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage).build());

        LOGGER.info("KafkaUser {}/{} is ready — asserting cert-manager certificate issuance",
            testStorage.getNamespaceName(), testStorage.getUsername());

        // The cert-manager managed Secret for the user follows the same -cm naming convention
        // as broker and cluster-operator Secrets: <username>-cm
        final String cmUserSecretName = testStorage.getUsername() + "-cm";
        final Secret cmUserSecret = KubeResourceManager.get().kubeClient().getClient()
            .secrets()
            .inNamespace(testStorage.getNamespaceName())
            .withName(cmUserSecretName)
            .get();

        assertThat("cert-manager user Secret '" + cmUserSecretName + "' must exist", cmUserSecret, notNullValue());

        final X509Certificate cmUserCert = SecretUtils.getCertificateFromSecret(cmUserSecret, "tls.crt");
        assertThat("cert-manager user cert must not be null", cmUserCert, notNullValue());

        // Retrieve the Strimzi user Secret and extract user.crt
        final Secret userSecret = KubeResourceManager.get().kubeClient().getClient()
            .secrets()
            .inNamespace(testStorage.getNamespaceName())
            .withName(testStorage.getUsername())
            .get();

        assertThat("Strimzi user Secret must exist", userSecret, notNullValue());

        final X509Certificate userCert = SecretUtils.getCertificateFromSecret(userSecret, "user.crt");
        assertThat("user.crt must not be null", userCert, notNullValue());

        assertThat("cert-manager Secret tls.crt must match Strimzi user Secret user.crt",
            cmUserCert, is(userCert));

        // Verify the user cert is signed by the cert-manager clients CA
        final Secret clientsCaCertSecret = KubeResourceManager.get().kubeClient().getClient()
            .secrets()
            .inNamespace(testStorage.getNamespaceName())
            .withName(KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName()))
            .get();

        assertThat("Clients CA cert Secret must exist", clientsCaCertSecret, notNullValue());

        final X509Certificate clientsCaCert = SecretUtils.getCertificateFromSecret(clientsCaCertSecret, Ca.CA_CRT);
        assertThat("Clients CA cert must not be null", clientsCaCert, notNullValue());

        assertThat("user.crt issuer DN must match the cert-manager clients CA subject DN",
            userCert.getIssuerX500Principal().getName(),
            is(clientsCaCert.getSubjectX500Principal().getName()));

        LOGGER.info("cert-manager user Secret '{}' cert matches Strimzi user Secret '{}', issuer '{}' matches clients CA subject '{}'",
            cmUserSecretName, testStorage.getUsername(),
            userCert.getIssuerX500Principal().getName(), clientsCaCert.getSubjectX500Principal().getName());

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

        LOGGER.info("TLS producer/consumer successfully exchanged {} messages after cert renewal rolling update",
                testStorage.getMessageCount());
    }

    @BeforeAll
    void setup() {
        SetupCertManager.deployCertManager();
        SetupCertManager.createIssuerAndCaSecret();
        SetupClusterOperator
            .getInstance()
            .install();
        SetupCertManager.installCertManagerRbac(SetupClusterOperator.getInstance().getOperatorNamespace());
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
