/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.custom;

import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.skodjob.kubetest4j.security.CertAndKey;
import io.skodjob.kubetest4j.security.CertAndKeyFiles;
import io.strimzi.api.kafka.model.common.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.security.SystemTestCertBundle;
import io.strimzi.systemtest.security.SystemTestCertGenerator;
import io.strimzi.systemtest.security.TrustChainSecrets;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.NetworkPolicyUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.security.cert.X509Certificate;
import java.util.List;

import static io.strimzi.systemtest.TestTags.CONNECT;
import static io.strimzi.systemtest.TestTags.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.security.SystemTestCertGenerator.exportToPemFiles;
import static io.strimzi.systemtest.security.SystemTestCertGenerator.generateEndEntityCertAndKey;
import static io.strimzi.systemtest.security.SystemTestCertGenerator.generateRootCaCertAndKey;
import static io.strimzi.systemtest.security.SystemTestCertGenerator.generateStrimziCaCertAndKey;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

@Tag(REGRESSION)
@SuiteDoc(
    description = @Desc("Test suite for verifying custom CA chain trust establishment, user certificate authentication with multi-stage CA hierarchies, and KafkaConnect trust chain configurations."),
    labels = {
        @Label(value = TestDocsLabels.SECURITY)
    }
)
public class CustomCaChainST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(CustomCaChainST.class);

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Verifies that only client certificates signed by the Clients CA Leaf are accepted by the operator-managed broker. Client certificates signed by Root CA or Intermediate CA are rejected even though they belong to the same Clients CA chain. This tests how the operator builds the brokers client-auth truststore from the custom Clients CA secret."),
        steps = {
            @Step(value = "Generate a custom CA chain: Root -> Intermediate -> Leaf.", expected = "CA chain is generated."),
            @Step(value = "Deploy the full chain as Cluster CA and Clients CA secrets.", expected = "CA secrets are deployed."),
            @Step(value = "Deploy Kafka cluster with custom CAs (generateCertificateAuthority: false).", expected = "Kafka cluster is ready."),
            @Step(value = "Create a KafkaTopic and a client certificate signed by the Clients CA Leaf. Verify that the client can produce and consume messages.", expected = "Messages are transmitted successfully."),
            @Step(value = "Create a client certificate signed by the Clients CA Root and verify it is rejected.", expected = "Producer/consumer time out due to TLS handshake failure."),
            @Step(value = "Create a client certificate signed by the Clients CA Intermediate and verify it is rejected.", expected = "Producer/consumer time out due to TLS handshake failure.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    void testMultistageCustomCaUserCertificateAuthentication() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        final SystemTestCertBundle clusterCa = SystemTestCertBundle.forClusterCa(testStorage);

        final SystemTestCertBundle clientsCa = SystemTestCertBundle.forClientsCa(testStorage);

        clusterCa.createCustomSecretsFromBundles(testStorage.getNamespaceName(), testStorage.getClusterName());
        clientsCa.createCustomSecretsFromBundles(testStorage.getNamespaceName(), testStorage.getClusterName());

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .withNewClusterCa()
                    .withGenerateCertificateAuthority(false)
                .endClusterCa()
                .withNewClientsCa()
                    .withGenerateCertificateAuthority(false)
                .endClientsCa()
            .endSpec()
            .build());

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());

        // i.) Create a client certificate signed by the Clients CA Leaf and verify it can connect
        LOGGER.info("Testing that client with Clients-CA-Leaf-signed certificate can connect");
        final String leafSignedClientName = "leaf-ca-signed-client";
        final CertAndKey leafSignedClient = generateEndEntityCertAndKey(clientsCa.getSystemTestCa(),
            SystemTestCertGenerator.retrieveKafkaBrokerSANs(testStorage.getNamespaceName(), testStorage.getClusterName()),
            "C=CZ, L=Prague, O=Strimzi, CN=" + leafSignedClientName);
        final CertAndKeyFiles leafSignedClientFiles = exportToPemFiles(leafSignedClient);
        SecretUtils.createCustomCertSecret(testStorage.getNamespaceName(), testStorage.getClusterName(),
            leafSignedClientName, leafSignedClientFiles, "user");

        final KafkaClients leafSignedClients = ClientUtils.getInstantTlsClientBuilder(testStorage)
            .withUsername(leafSignedClientName)
            .build();
        KubeResourceManager.get().createResourceWithWait(
            leafSignedClients.producerTlsStrimzi(testStorage.getClusterName()),
            leafSignedClients.consumerTlsStrimzi(testStorage.getClusterName())
        );
        ClientUtils.waitForInstantClientSuccess(testStorage);

        // ii.) Create a client certificate signed by the Clients CA Root and verify it is rejected
        LOGGER.info("Testing that client with Root-CA-signed certificate is rejected");
        final String rootSignedClientName = "root-ca-signed-client";
        final CertAndKey rootSignedClient = generateEndEntityCertAndKey(clientsCa.getStrimziRootCa(),
            SystemTestCertGenerator.retrieveKafkaBrokerSANs(testStorage.getNamespaceName(), testStorage.getClusterName()),
            "C=CZ, L=Prague, O=Strimzi, CN=" + rootSignedClientName);
        final CertAndKeyFiles rootSignedClientFiles = exportToPemFiles(rootSignedClient);
        SecretUtils.createCustomCertSecret(testStorage.getNamespaceName(), testStorage.getClusterName(),
            rootSignedClientName, rootSignedClientFiles, "user");

        final KafkaClients rootSignedClients = ClientUtils.getInstantTlsClientBuilder(testStorage)
            .withUsername(rootSignedClientName)
            .build();
        KubeResourceManager.get().createResourceWithWait(
            rootSignedClients.producerTlsStrimzi(testStorage.getClusterName()),
            rootSignedClients.consumerTlsStrimzi(testStorage.getClusterName())
        );
        ClientUtils.waitForInstantClientsTimeout(testStorage);

        // iii.) Create a client certificate signed by the Clients CA Intermediate and verify it is rejected
        LOGGER.info("Testing that client with Intermediate-CA-signed certificate is rejected");
        final String intermediateSignedClientName = "intermediate-ca-signed-client";
        final CertAndKey intermediateSignedClient = generateEndEntityCertAndKey(clientsCa.getIntermediateCa(),
            SystemTestCertGenerator.retrieveKafkaBrokerSANs(testStorage.getNamespaceName(), testStorage.getClusterName()),
            "C=CZ, L=Prague, O=Strimzi, CN=" + intermediateSignedClientName);
        final CertAndKeyFiles intermediateSignedClientFiles = exportToPemFiles(intermediateSignedClient);
        SecretUtils.createCustomCertSecret(testStorage.getNamespaceName(), testStorage.getClusterName(),
            intermediateSignedClientName, intermediateSignedClientFiles, "user");

        final KafkaClients intermediateSignedClients = ClientUtils.getInstantTlsClientBuilder(testStorage)
            .withUsername(intermediateSignedClientName)
            .build();
        KubeResourceManager.get().createResourceWithWait(
            intermediateSignedClients.producerTlsStrimzi(testStorage.getClusterName()),
            intermediateSignedClients.consumerTlsStrimzi(testStorage.getClusterName())
        );
        ClientUtils.waitForInstantClientsTimeout(testStorage);
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Verifies that clients can establish trust based on any issuer from the custom CA chain the Leaf CA, Intermediate CA, or the Root CA when the broker presents the full certificate chain. A foreign CA that is not part of the chain should fail to establish trust."),
        steps = {
            @Step(value = "Generate a custom CA chain: Root -> Intermediate -> Leaf.", expected = "CA chain is generated."),
            @Step(value = "Deploy the full chain as Cluster CA and Clients CA secrets.", expected = "CA secrets are deployed."),
            @Step(value = "Deploy Kafka cluster with custom CAs.", expected = "Kafka cluster is ready."),
            @Step(value = "Verify the broker certificate chain contains 4 certificates and validate the issuer chain: broker cert -> Leaf CA -> Intermediate CA -> Root CA (self-signed).", expected = "Chain contains 4 certs with correct issuer relationships and CA basic constraints."),
            @Step(value = "Create five trust secrets with different levels: Root + Intermediate + Leaf, Root + Intermediate, Root only, Intermediate only, Leaf only.", expected = "Trust secrets are created."),
            @Step(value = "For each trust secret, verify that clients can successfully produce and consume messages.", expected = "All five trust configurations succeed."),
            @Step(value = "Create a trust secret with only a foreign Root CA.", expected = "Foreign trust secret is created."),
            @Step(value = "Verify that clients using the foreign CA trust secret cannot connect.", expected = "Producer/consumer time out due to trust failure.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    void testMultistageCustomCaTrustChainEstablishment() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        final SystemTestCertBundle clusterCa = SystemTestCertBundle.forClusterCa(testStorage);

        final SystemTestCertBundle clientsCa = SystemTestCertBundle.forClientsCa(testStorage);

        clusterCa.createCustomSecretsFromBundles(testStorage.getNamespaceName(), testStorage.getClusterName());
        clientsCa.createCustomSecretsFromBundles(testStorage.getNamespaceName(), testStorage.getClusterName());

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .withNewClusterCa()
                    .withGenerateCertificateAuthority(false)
                .endClusterCa()
                .withNewClientsCa()
                    .withGenerateCertificateAuthority(false)
                .endClientsCa()
            .endSpec()
            .build());

        //  Verify broker certificate chain: broker cert -> Leaf CA -> Intermediate CA -> Root CA
        LOGGER.info("Verifying broker certificate chain contains all certificates");
        final String brokerPodName = KubeResourceManager.get().kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector()).get(0).getMetadata().getName();
        final List<X509Certificate> brokerChain = SecretUtils.getCertificatesFromSecret(
            KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(brokerPodName).get(),
            brokerPodName + ".crt");
        LOGGER.info("Broker certificate chain contains {} certificates", brokerChain.size());
        assertThat("Broker certificate chain should contain 4 certs: broker cert, Leaf CA, Intermediate CA, Root CA",
            brokerChain.size(), is(4));

        final X509Certificate brokerCert = brokerChain.get(0);
        final X509Certificate leafCaCert = brokerChain.get(1);
        final X509Certificate intermediateCaCert = brokerChain.get(2);
        final X509Certificate rootCaCert = brokerChain.get(3);

        assertThat("Broker cert should be issued by Leaf CA", brokerCert.getIssuerX500Principal(), is(leafCaCert.getSubjectX500Principal()));
        assertThat("Leaf CA cert should be issued by Intermediate CA", leafCaCert.getIssuerX500Principal(), is(intermediateCaCert.getSubjectX500Principal()));
        assertThat("Leaf CA cert should be a CA", leafCaCert.getBasicConstraints(), is(greaterThanOrEqualTo(0)));
        assertThat("Intermediate CA cert should be issued by Root CA", intermediateCaCert.getIssuerX500Principal(), is(rootCaCert.getSubjectX500Principal()));
        assertThat("Intermediate CA cert should be a CA", intermediateCaCert.getBasicConstraints(), is(greaterThanOrEqualTo(0)));
        assertThat("Root CA cert should be self-signed", rootCaCert.getIssuerX500Principal(), is(rootCaCert.getSubjectX500Principal()));
        assertThat("Root CA cert should be a CA", rootCaCert.getBasicConstraints(), is(greaterThanOrEqualTo(0)));

        //  Create KafkaUser and Topic
        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());
        KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage).build());

        //  Create trust secrets with different chain levels
        final CertAndKey foreignRootCa = generateRootCaCertAndKey("C=CZ, L=Prague, O=Foreign, CN=ForeignRootCA", null);

        TrustChainSecrets.fromBundle(clusterCa)
            .withFullChain(TrustChainSecrets.TRUST_FULL_CHAIN)
            .withRootAndIntermediate(TrustChainSecrets.TRUST_ROOT_INTERMEDIATE)
            .withRootOnly(TrustChainSecrets.TRUST_ROOT_ONLY)
            .withIntermediateOnly(TrustChainSecrets.TRUST_INTERMEDIATE_ONLY)
            .withLeafOnly(TrustChainSecrets.TRUST_LEAF_ONLY)
            .withCustom(TrustChainSecrets.TRUST_FOREIGN, foreignRootCa)
            .build(testStorage.getNamespaceName(), testStorage.getClusterName());

        //  Verify trust with each chain level
        for (String trustSecretName : List.of(TrustChainSecrets.TRUST_FULL_CHAIN, TrustChainSecrets.TRUST_ROOT_INTERMEDIATE,
                TrustChainSecrets.TRUST_ROOT_ONLY, TrustChainSecrets.TRUST_INTERMEDIATE_ONLY, TrustChainSecrets.TRUST_LEAF_ONLY)) {
            LOGGER.info("Testing trust establishment with trust secret: {}", trustSecretName);
            final KafkaClients kafkaClients = ClientUtils.getInstantTlsClientBuilder(testStorage)
                .withCaCertSecretName(trustSecretName)
                .build();
            KubeResourceManager.get().createResourceWithWait(
                kafkaClients.producerTlsStrimzi(testStorage.getClusterName()),
                kafkaClients.consumerTlsStrimzi(testStorage.getClusterName())
            );
            ClientUtils.waitForInstantClientSuccess(testStorage);
        }

        //  Verify foreign CA trust fails
        LOGGER.info("Testing that foreign CA trust secret fails to establish trust");
        final KafkaClients foreignClients = ClientUtils.getInstantTlsClientBuilder(testStorage)
            .withCaCertSecretName(TrustChainSecrets.TRUST_FOREIGN)
            .build();
        KubeResourceManager.get().createResourceWithWait(
            foreignClients.producerTlsStrimzi(testStorage.getClusterName()),
            foreignClients.consumerTlsStrimzi(testStorage.getClusterName())
        );
        ClientUtils.waitForInstantClientsTimeout(testStorage);
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Verifies that only client certificates signed by the Leaf CA are accepted on the internal listener (port 9091). Client certificates signed by Root CA or Intermediate CA are rejected even though they belong to the same CA chain."),
        steps = {
            @Step(value = "Generate a custom CA chain: Root -> Intermediate -> Leaf.", expected = "CA chain is generated."),
            @Step(value = "Deploy the full custom CA chain as Cluster CA and Clients CA secrets.", expected = "CA secrets are deployed."),
            @Step(value = "Deploy Kafka cluster with custom CAs (generateCertificateAuthority: false) so that broker certificates are signed by the Leaf CA.", expected = "Kafka cluster is ready."),
            @Step(value = "Create a NetworkPolicy allowing all ingress to Kafka broker pods so that test clients can reach port 9091.", expected = "NetworkPolicy is created."),
            @Step(value = "Create a KafkaTopic and a client certificate signed by the Leaf CA. Verify that the client can produce and consume messages on port 9091.", expected = "Messages are transmitted successfully."),
            @Step(value = "Create a client certificate signed by the Root CA and verify it is rejected on port 9091.", expected = "Producer/consumer time out due to TLS handshake failure."),
            @Step(value = "Create a client certificate signed by the Intermediate CA and verify it is rejected on port 9091.", expected = "Producer/consumer time out due to TLS handshake failure.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    void testCustomCaTrustChainOnInternalPort() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        final SystemTestCertBundle clusterCa = SystemTestCertBundle.forClusterCa(testStorage);

        final SystemTestCertBundle clientsCa = SystemTestCertBundle.forClientsCa(testStorage);

        clusterCa.createCustomSecretsFromBundles(testStorage.getNamespaceName(), testStorage.getClusterName());
        clientsCa.createCustomSecretsFromBundles(testStorage.getNamespaceName(), testStorage.getClusterName());

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .withNewClusterCa()
                    .withGenerateCertificateAuthority(false)
                .endClusterCa()
                .withNewClientsCa()
                    .withGenerateCertificateAuthority(false)
                .endClientsCa()
            .endSpec()
            .build());

        // Allow all ingress to broker pods so that test client pods can access port 9091
        // The operators default NetworkPolicy restricts port 9091 to internal Strimzi components only
        NetworkPolicyUtils.allowNetworkPolicyAllIngressForMatchingLabel(testStorage.getNamespaceName(),
            testStorage.getClusterName() + "-internal-port-access",
            testStorage.getBrokerPoolSelector().getMatchLabels());

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());

        // Client-side trust secret with full chain so that test clients can verify the broker's certificate
        TrustChainSecrets.fromBundle(clusterCa)
            .withFullChain(TrustChainSecrets.TRUST_FULL_CHAIN)
            .build(testStorage.getNamespaceName(), testStorage.getClusterName());

        final String internalBootstrapAddress = KafkaResources.brokersServiceName(testStorage.getClusterName()) + ":9091";

        // i.) Create a client certificate signed by the Leaf CA and verify it can connect on port 9091
        //     Port 9091 (replication listener) trusts only the Cluster CA for client authentication,
        //     not the Clients CA. We must generate a client certificate signed by the Cluster CA leaf.
        LOGGER.info("Testing that client with Leaf-CA-signed certificate can connect on port 9091");
        final String leafSignedClientName = "leaf-ca-signed-client";
        final CertAndKey leafSignedClient = generateEndEntityCertAndKey(clusterCa.getSystemTestCa(),
            SystemTestCertGenerator.retrieveKafkaBrokerSANs(testStorage.getNamespaceName(), testStorage.getClusterName()),
            "C=CZ, L=Prague, O=Strimzi, CN=" + leafSignedClientName);
        final CertAndKeyFiles leafSignedClientFiles = exportToPemFiles(leafSignedClient);
        SecretUtils.createCustomCertSecret(testStorage.getNamespaceName(), testStorage.getClusterName(),
            leafSignedClientName, leafSignedClientFiles, "user");

        final KafkaClients leafSignedClients = ClientUtils.getInstantTlsClientBuilder(testStorage, internalBootstrapAddress)
            .withUsername(leafSignedClientName)
            .withCaCertSecretName(TrustChainSecrets.TRUST_FULL_CHAIN)
            .build();
        KubeResourceManager.get().createResourceWithWait(
            leafSignedClients.producerTlsStrimzi(testStorage.getClusterName()),
            leafSignedClients.consumerTlsStrimzi(testStorage.getClusterName())
        );
        ClientUtils.waitForInstantClientSuccess(testStorage);

        // ii.) Create a client certificate signed by the Root CA and verify it is rejected on port 9091
        LOGGER.info("Testing that client with Root-CA-signed certificate is rejected on port 9091");
        final String rootSignedClientName = "root-ca-signed-client";
        final CertAndKey rootSignedClient = generateEndEntityCertAndKey(clusterCa.getStrimziRootCa(),
            SystemTestCertGenerator.retrieveKafkaBrokerSANs(testStorage.getNamespaceName(), testStorage.getClusterName()),
            "C=CZ, L=Prague, O=Strimzi, CN=" + rootSignedClientName);
        final CertAndKeyFiles rootSignedClientFiles = exportToPemFiles(rootSignedClient);
        SecretUtils.createCustomCertSecret(testStorage.getNamespaceName(), testStorage.getClusterName(),
            rootSignedClientName, rootSignedClientFiles, "user");

        final KafkaClients rootSignedClients = ClientUtils.getInstantTlsClientBuilder(testStorage, internalBootstrapAddress)
            .withUsername(rootSignedClientName)
            .withCaCertSecretName(TrustChainSecrets.TRUST_FULL_CHAIN)
            .build();
        KubeResourceManager.get().createResourceWithWait(
            rootSignedClients.producerTlsStrimzi(testStorage.getClusterName()),
            rootSignedClients.consumerTlsStrimzi(testStorage.getClusterName())
        );
        ClientUtils.waitForInstantClientsTimeout(testStorage);

        // iii.) Create a client certificate signed by the Intermediate CA and verify it is rejected on port 9091
        LOGGER.info("Testing that client with Intermediate-CA-signed certificate is rejected on port 9091");
        final String intermediateSignedClientName = "intermediate-ca-signed-client";
        final CertAndKey intermediateSignedClient = generateEndEntityCertAndKey(clusterCa.getIntermediateCa(),
            SystemTestCertGenerator.retrieveKafkaBrokerSANs(testStorage.getNamespaceName(), testStorage.getClusterName()),
            "C=CZ, L=Prague, O=Strimzi, CN=" + intermediateSignedClientName);
        final CertAndKeyFiles intermediateSignedClientFiles = exportToPemFiles(intermediateSignedClient);
        SecretUtils.createCustomCertSecret(testStorage.getNamespaceName(), testStorage.getClusterName(),
            intermediateSignedClientName, intermediateSignedClientFiles, "user");

        final KafkaClients intermediateSignedClients = ClientUtils.getInstantTlsClientBuilder(testStorage, internalBootstrapAddress)
            .withUsername(intermediateSignedClientName)
            .withCaCertSecretName(TrustChainSecrets.TRUST_FULL_CHAIN)
            .build();
        KubeResourceManager.get().createResourceWithWait(
            intermediateSignedClients.producerTlsStrimzi(testStorage.getClusterName()),
            intermediateSignedClients.consumerTlsStrimzi(testStorage.getClusterName())
        );
        ClientUtils.waitForInstantClientsTimeout(testStorage);
    }

    @ParallelNamespaceTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    @TestDoc(
        description = @Desc("Verifies that KafkaConnect properly establishes trust when connecting to Kafka using valid custom CA configurations, and fails to connect when an untrusted (Subleaf) CA is used."),
        steps = {
            @Step(value = "Generate a custom CA chain: Root -> Intermediate -> Leaf.", expected = "CA chain is generated."),
            @Step(value = "Generate a Subleaf CA signed by Leaf (Root -> Intermediate -> Leaf -> Subleaf).", expected = "Subleaf CA is generated."),
            @Step(value = "Deploy the full custom CA chain as Cluster CA secrets.", expected = "Cluster CA secrets are deployed."),
            @Step(value = "Deploy Kafka cluster with custom Cluster CA so that broker certificates are signed by the Leaf CA.", expected = "Kafka cluster is ready."),
            @Step(value = "Create six trust secrets for KafkaConnect: Root + Intermediate + Leaf, Root + Intermediate, Root only, Intermediate only, Leaf only, and Subleaf chain.", expected = "Trust secrets are created."),
            @Step(value = "For each valid trust secret (Root + Intermediate + Leaf, Root + Intermediate, Root only, Intermediate only, Leaf only), deploy KafkaConnect and verify it becomes ready.", expected = "KafkaConnect connects successfully."),
            @Step(value = "Deploy KafkaConnect with the Subleaf trust secret and verify it does not become ready.", expected = "KafkaConnect fails to connect.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    void testKafkaConnectTrustWithCustomCaChain() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        final SystemTestCertBundle clusterCa = SystemTestCertBundle.forClusterCa(testStorage);

        final SystemTestCertBundle clientsCa = SystemTestCertBundle.forClientsCa(testStorage);

        // Generate a Subleaf CA signed by the Leaf CA (one level deeper)
        final CertAndKey subleafCa = generateStrimziCaCertAndKey(clusterCa.getSystemTestCa(), "C=CZ, L=Prague, O=StrimziTest, CN=SubleafCA");

        clusterCa.createCustomSecretsFromBundles(testStorage.getNamespaceName(), testStorage.getClusterName());
        clientsCa.createCustomSecretsFromBundles(testStorage.getNamespaceName(), testStorage.getClusterName());

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .withNewClusterCa()
                    .withGenerateCertificateAuthority(false)
                .endClusterCa()
                .withNewClientsCa()
                    .withGenerateCertificateAuthority(false)
                .endClientsCa()
            .endSpec()
            .build());

        //  Create trust secrets for KafkaConnect
        TrustChainSecrets.fromBundle(clusterCa)
            .withFullChain(TrustChainSecrets.TRUST_FULL_CHAIN)
            .withRootAndIntermediate(TrustChainSecrets.TRUST_ROOT_INTERMEDIATE)
            .withRootOnly(TrustChainSecrets.TRUST_ROOT_ONLY)
            .withIntermediateOnly(TrustChainSecrets.TRUST_INTERMEDIATE_ONLY)
            .withLeafOnly(TrustChainSecrets.TRUST_LEAF_ONLY)
            .withCustom(TrustChainSecrets.TRUST_SUBLEAF, subleafCa, clusterCa.getSystemTestCa(), clusterCa.getIntermediateCa(), clusterCa.getStrimziRootCa())
            .build(testStorage.getNamespaceName(), testStorage.getClusterName());

        // Positive cases: KafkaConnect should become ready
        // Deploy one at a time to avoid multiple KafkaConnect instances in the same namespace
        for (String trustSecretName : List.of(TrustChainSecrets.TRUST_FULL_CHAIN, TrustChainSecrets.TRUST_ROOT_INTERMEDIATE,
                TrustChainSecrets.TRUST_ROOT_ONLY, TrustChainSecrets.TRUST_INTERMEDIATE_ONLY, TrustChainSecrets.TRUST_LEAF_ONLY)) {

            final String connectClusterName = testStorage.getClusterName() + "-connect-" + trustSecretName;
            LOGGER.info("Deploying KafkaConnect with trust secret: {}", trustSecretName);

            final KafkaConnect kafkaConnect = KafkaConnectTemplates.kafkaConnect(testStorage.getNamespaceName(), connectClusterName, testStorage.getClusterName(), 1)
                .editOrNewSpec()
                    .withNewTls()
                        .withTrustedCertificates(
                            new CertSecretSourceBuilder()
                                .withSecretName(trustSecretName)
                                .withCertificate("ca.crt")
                                .build()
                        )
                    .endTls()
                .endSpec()
                .build();

            KubeResourceManager.get().createResourceWithoutWait(kafkaConnect);
            KafkaConnectUtils.waitForConnectReady(testStorage.getNamespaceName(), connectClusterName);
            LOGGER.info("KafkaConnect with trust secret {} is ready, deleting before next iteration", trustSecretName);
            KubeResourceManager.get().deleteResourceWithWait(kafkaConnect);
        }

        //  Negative case: Subleaf trust secret should fail
        final String connectSubleafClusterName = testStorage.getClusterName() + "-connect-subleaf";
        LOGGER.info("Deploying KafkaConnect with Subleaf trust secret (should fail)");
        // i.e. this should be in the logs of the KafkaConnect:
        //  (certificate_unknown) PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException:
        //  unable to find valid certification path to requested target
        // Plus org.apache.kafka.common.errors.SslAuthenticationException: SSL handshake failed

        KubeResourceManager.get().createResourceWithoutWait(
            KafkaConnectTemplates.kafkaConnect(testStorage.getNamespaceName(), connectSubleafClusterName, testStorage.getClusterName(), 1)
                .editOrNewSpec()
                    .withNewTls()
                        .withTrustedCertificates(
                            new CertSecretSourceBuilder()
                                .withSecretName(TrustChainSecrets.TRUST_SUBLEAF)
                                .withCertificate("ca.crt")
                                .build()
                        )
                    .endTls()
                .endSpec()
                .build());

        KafkaConnectUtils.waitForConnectNotReady(testStorage.getNamespaceName(), connectSubleafClusterName);
        LOGGER.info("KafkaConnect with Subleaf trust secret correctly failed to become ready");
    }

    @BeforeAll
    void setup() {
        SetupClusterOperator
            .getInstance()
            .withDefaultConfiguration()
            .install();
    }
}
