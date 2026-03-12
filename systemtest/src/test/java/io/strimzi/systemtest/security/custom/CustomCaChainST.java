/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.custom;

import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.skodjob.kubetest4j.security.CertAndKey;
import io.skodjob.kubetest4j.security.CertAndKeyFiles;
import io.strimzi.api.kafka.model.common.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.common.template.AdditionalVolumeBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.security.SystemTestCertBundle;
import io.strimzi.systemtest.security.SystemTestCertGenerator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import org.apache.kafka.common.config.SslClientAuth;
import org.apache.kafka.common.config.SslConfigs;
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
import static io.strimzi.systemtest.security.SystemTestCertGenerator.generateIntermediateCaCertAndKey;
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
        description = @Desc("Verifies that only users with certificates signed by the designated Leaf CA can connect to Kafka. " +
            "Users with certificates signed by Intermediate CA, Root CA, or a foreign CA are rejected."),
        steps = {
            @Step(value = "Generate a custom CA chain: Root -> Intermediate -> Leaf.", expected = "CA chain is generated."),
            @Step(value = "Generate a separate foreign Root CA.", expected = "Foreign CA is generated."),
            @Step(value = "Generate four user certificates signed by Leaf CA, Intermediate CA, Root CA, and foreign CA respectively.", expected = "User certificates are generated."),
            @Step(value = "Deploy user cert secrets and a CA trust secret containing only the Leaf CA cert.", expected = "Secrets are created in the namespace."),
            @Step(value = "Deploy Kafka with a custom listener (port 9122) configured with ssl.client.auth=required and PEM truststore pointing to the Leaf CA.", expected = "Kafka cluster is ready with custom listener."),
            @Step(value = "Verify that the user with a Leaf-CA-signed cert can produce and consume messages.", expected = "Messages are transmitted successfully."),
            @Step(value = "Verify that users with Intermediate-CA, Root-CA, and foreign-CA-signed certs are rejected.", expected = "Producer/consumer time out due to TLS handshake failure.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    void testMultistageCustomCaUserCertificateAuthentication() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        //  Generate CA chain: Root -> Intermediate -> Leaf
        final CertAndKey rootCa = generateRootCaCertAndKey();
        final CertAndKey intermediateCa = generateIntermediateCaCertAndKey(rootCa);
        final CertAndKey leafCa = generateStrimziCaCertAndKey(intermediateCa, "C=CZ, L=Prague, O=StrimziTest, CN=LeafCA");

        // Generate a foreign (unrelated) Root CA
        final CertAndKey foreignRootCa = generateRootCaCertAndKey("C=CZ, L=Prague, O=Foreign, CN=ForeignRootCA", null);

        //  Generate user certificates signed by different CAs
        final String userLeafSignedName = "user-leaf-signed";
        final String userIntermediateSignedName = "user-intermediate-signed";
        final String userRootSignedName = "user-root-signed";
        final String userForeignSignedName = "user-foreign-signed";
        final String userKeyInSecret = "user";

        final CertAndKey userLeafSigned = generateEndEntityCertAndKey(leafCa,
            SystemTestCertGenerator.retrieveKafkaBrokerSANs(testStorage),
            "C=CZ, L=Prague, O=Strimzi, CN=" + userLeafSignedName);
        final CertAndKey userIntermediateSigned = generateEndEntityCertAndKey(intermediateCa,
            SystemTestCertGenerator.retrieveKafkaBrokerSANs(testStorage),
            "C=CZ, L=Prague, O=Strimzi, CN=" + userIntermediateSignedName);
        final CertAndKey userRootSigned = generateEndEntityCertAndKey(rootCa,
            SystemTestCertGenerator.retrieveKafkaBrokerSANs(testStorage),
            "C=CZ, L=Prague, O=Strimzi, CN=" + userRootSignedName);
        final CertAndKey userForeignSigned = generateEndEntityCertAndKey(foreignRootCa,
            SystemTestCertGenerator.retrieveKafkaBrokerSANs(testStorage),
            "C=CZ, L=Prague, O=Strimzi, CN=" + userForeignSignedName);

        //  Export to PEM files and create K8s secrets
        final CertAndKeyFiles leafSignedFiles = exportToPemFiles(userLeafSigned);
        final CertAndKeyFiles intermediateSignedFiles = exportToPemFiles(userIntermediateSigned);
        final CertAndKeyFiles rootSignedFiles = exportToPemFiles(userRootSigned);
        final CertAndKeyFiles foreignSignedFiles = exportToPemFiles(userForeignSigned);

        // CA trust secret containing only the Leaf CA cert
        final String customCaCertName = "custom-leaf-ca";
        final CertAndKeyFiles leafCaCertFiles = exportToPemFiles(leafCa);

        SecretUtils.createCustomCertSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), userLeafSignedName, leafSignedFiles, userKeyInSecret);
        SecretUtils.createCustomCertSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), userIntermediateSignedName, intermediateSignedFiles, userKeyInSecret);
        SecretUtils.createCustomCertSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), userRootSignedName, rootSignedFiles, userKeyInSecret);
        SecretUtils.createCustomCertSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), userForeignSignedName, foreignSignedFiles, userKeyInSecret);
        SecretUtils.createCustomCertSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), customCaCertName, leafCaCertFiles);

        // Deploy Kafka with custom listener
        final String mountPath = "/mnt/kafka/custom-authn-secrets/my-listener";

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .editTemplate()
                        .editPod()
                            .addToVolumes(new AdditionalVolumeBuilder()
                                .withName(customCaCertName)
                                .withSecret(new SecretVolumeSourceBuilder()
                                    .withSecretName(customCaCertName)
                                    .build())
                                .build())
                        .endPod()
                        .editKafkaContainer()
                            .addToVolumeMounts(new VolumeMountBuilder()
                                .withName(customCaCertName)
                                .withMountPath(mountPath + "/" + customCaCertName)
                                .build())
                        .endKafkaContainer()
                    .endTemplate()
                    .withListeners(new GenericKafkaListenerBuilder()
                        .withType(KafkaListenerType.INTERNAL)
                        .withName("custom")
                        .withPort(9122)
                        .withTls(true)
                        .withNewKafkaListenerAuthenticationCustomAuth()
                            .withSasl(false)
                            .addToListenerConfig("ssl.client.auth", SslClientAuth.REQUIRED.toString())
                            .addToListenerConfig(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, mountPath + "/" + customCaCertName + "/ca.crt")
                            .addToListenerConfig(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM")
                        .endKafkaListenerAuthenticationCustomAuth()
                        .build())
                .endKafka()
            .endSpec()
            .build());

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());

        final String bootstrapAddress = KafkaResources.bootstrapServiceName(testStorage.getClusterName()) + ":9122";

        // i.) Leaf-CA-signed user should succeed
        LOGGER.info("Testing that user with Leaf-CA-signed certificate can connect");
        final KafkaClients kafkaClients = ClientUtils.getInstantTlsClients(testStorage, bootstrapAddress);
        kafkaClients.setUsername(userLeafSignedName);
        KubeResourceManager.get().createResourceWithWait(
            kafkaClients.producerTlsStrimzi(testStorage.getClusterName()),
            kafkaClients.consumerTlsStrimzi(testStorage.getClusterName())
        );
        ClientUtils.waitForInstantClientSuccess(testStorage);

        // ii.) Intermediate-CA-signed user should be rejected
        // i.e.,
        // org.apache.kafka.common.errors.SslAuthenticationException: Failed to process post-handshake messages                                       │
        // Caused by: javax.net.ssl.SSLHandshakeException: Received fatal alert: certificate_required
        LOGGER.info("Testing that user with Intermediate-CA-signed certificate is rejected");
        kafkaClients.setUsername(userIntermediateSignedName);
        KubeResourceManager.get().createResourceWithWait(
            kafkaClients.producerTlsStrimzi(testStorage.getClusterName()),
            kafkaClients.consumerTlsStrimzi(testStorage.getClusterName())
        );
        ClientUtils.waitForInstantClientsTimeout(testStorage);

        // iii.) Root-CA-signed user should be rejected
        LOGGER.info("Testing that user with Root-CA-signed certificate is rejected");
        kafkaClients.setUsername(userRootSignedName);
        KubeResourceManager.get().createResourceWithWait(
            kafkaClients.producerTlsStrimzi(testStorage.getClusterName()),
            kafkaClients.consumerTlsStrimzi(testStorage.getClusterName())
        );
        ClientUtils.waitForInstantClientsTimeout(testStorage);

        // iv.) Foreign-CA-signed user should be rejected
        LOGGER.info("Testing that user with foreign-CA-signed certificate is rejected");
        kafkaClients.setUsername(userForeignSignedName);
        KubeResourceManager.get().createResourceWithWait(
            kafkaClients.producerTlsStrimzi(testStorage.getClusterName()),
            kafkaClients.consumerTlsStrimzi(testStorage.getClusterName())
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

        final SystemTestCertBundle clusterCa = new SystemTestCertBundle(
            "CN=" + testStorage.getTestName() + "ClusterCA",
            KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()),
            KafkaResources.clusterCaKeySecretName(testStorage.getClusterName()));

        final SystemTestCertBundle clientsCa = new SystemTestCertBundle(
            "CN=" + testStorage.getTestName() + "ClientsCA",
            KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName()),
            KafkaResources.clientsCaKeySecretName(testStorage.getClusterName()));

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
        final String trustFullChainName = "trust-full-chain";
        final String trustRootIntermediateName = "trust-root-intermediate";
        final String trustRootOnlyName = "trust-root-only";
        final String trustIntermediateOnlyName = "trust-intermediate-only";
        final String trustLeafOnlyName = "trust-leaf-only";
        final String trustForeignName = "trust-foreign";

        final CertAndKeyFiles fullChainFiles = exportToPemFiles(clusterCa.getSystemTestCa(), clusterCa.getIntermediateCa(), clusterCa.getStrimziRootCa());
        final CertAndKeyFiles rootIntermediateFiles = exportToPemFiles(clusterCa.getIntermediateCa(), clusterCa.getStrimziRootCa());
        final CertAndKeyFiles rootOnlyFiles = exportToPemFiles(clusterCa.getStrimziRootCa());
        final CertAndKeyFiles intermediateOnlyFiles = exportToPemFiles(clusterCa.getIntermediateCa());
        final CertAndKeyFiles leafOnlyFiles = exportToPemFiles(clusterCa.getSystemTestCa());

        SecretUtils.createCustomCertSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), trustFullChainName, fullChainFiles);
        SecretUtils.createCustomCertSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), trustRootIntermediateName, rootIntermediateFiles);
        SecretUtils.createCustomCertSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), trustRootOnlyName, rootOnlyFiles);
        SecretUtils.createCustomCertSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), trustIntermediateOnlyName, intermediateOnlyFiles);
        SecretUtils.createCustomCertSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), trustLeafOnlyName, leafOnlyFiles);

        // Generate foreign Root CA
        final CertAndKey foreignRootCa = generateRootCaCertAndKey("C=CZ, L=Prague, O=Foreign, CN=ForeignRootCA", null);
        final CertAndKeyFiles foreignCaFiles = exportToPemFiles(foreignRootCa);
        SecretUtils.createCustomCertSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), trustForeignName, foreignCaFiles);

        //  Verify trust with each chain level
        final String[] trustSecretNames = {trustFullChainName, trustRootIntermediateName, trustRootOnlyName, trustIntermediateOnlyName, trustLeafOnlyName};
        for (String trustSecretName : trustSecretNames) {
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
            .withCaCertSecretName(trustForeignName)
            .build();
        KubeResourceManager.get().createResourceWithWait(
            foreignClients.producerTlsStrimzi(testStorage.getClusterName()),
            foreignClients.consumerTlsStrimzi(testStorage.getClusterName())
        );
        ClientUtils.waitForInstantClientsTimeout(testStorage);
    }

    @ParallelNamespaceTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    @TestDoc(
        description = @Desc("Verifies that KafkaConnect properly establishes trust when connecting to Kafka using various custom CA configurations."),
        steps = {
            @Step(value = "Generate a custom CA chain: Root -> Intermediate -> Leaf.", expected = "CA chain is generated."),
            @Step(value = "Generate a Subleaf CA signed by Leaf (Root -> Intermediate -> Leaf -> Subleaf).", expected = "Subleaf CA is generated."),
            @Step(value = "Deploy the full chain as Cluster CA secrets.", expected = "Cluster CA secrets are deployed."),
            @Step(value = "Deploy Kafka cluster with custom Cluster CA.", expected = "Kafka cluster is ready."),
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

        final SystemTestCertBundle clusterCa = new SystemTestCertBundle(
            "CN=" + testStorage.getTestName() + "ClusterCA",
            KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()),
            KafkaResources.clusterCaKeySecretName(testStorage.getClusterName()));

        final SystemTestCertBundle clientsCa = new SystemTestCertBundle(
            "CN=" + testStorage.getTestName() + "ClientsCA",
            KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName()),
            KafkaResources.clientsCaKeySecretName(testStorage.getClusterName()));

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
        final String connectTrustFullName = "trust-full";
        final String connectTrustRootIntermediateName = "trust-root-im";
        final String connectTrustRootName = "trust-root";
        final String connectTrustIntermediateOnlyName = "trust-im-only";
        final String connectTrustLeafOnlyName = "trust-leaf-only";
        final String connectTrustSubleafName = "trust-subleaf";

        final CertAndKeyFiles fullChainFiles = exportToPemFiles(clusterCa.getSystemTestCa(), clusterCa.getIntermediateCa(), clusterCa.getStrimziRootCa());
        final CertAndKeyFiles rootIntermediateFiles = exportToPemFiles(clusterCa.getIntermediateCa(), clusterCa.getStrimziRootCa());
        final CertAndKeyFiles rootOnlyFiles = exportToPemFiles(clusterCa.getStrimziRootCa());
        final CertAndKeyFiles intermediateOnlyFiles = exportToPemFiles(clusterCa.getIntermediateCa());
        final CertAndKeyFiles leafOnlyFiles = exportToPemFiles(clusterCa.getSystemTestCa());
        final CertAndKeyFiles subleafChainFiles = exportToPemFiles(subleafCa, clusterCa.getSystemTestCa(), clusterCa.getIntermediateCa(), clusterCa.getStrimziRootCa());

        SecretUtils.createCustomCertSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), connectTrustFullName, fullChainFiles);
        SecretUtils.createCustomCertSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), connectTrustRootIntermediateName, rootIntermediateFiles);
        SecretUtils.createCustomCertSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), connectTrustRootName, rootOnlyFiles);
        SecretUtils.createCustomCertSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), connectTrustIntermediateOnlyName, intermediateOnlyFiles);
        SecretUtils.createCustomCertSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), connectTrustLeafOnlyName, leafOnlyFiles);
        SecretUtils.createCustomCertSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), connectTrustSubleafName, subleafChainFiles);

        // Positive cases: KafkaConnect should become ready
        // Deploy one at a time to avoid multiple KafkaConnect instances in the same namespace
        final String[] positiveSecrets = {connectTrustFullName, connectTrustRootIntermediateName, connectTrustRootName, connectTrustIntermediateOnlyName, connectTrustLeafOnlyName};
        for (String trustSecretName : positiveSecrets) {

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
                                .withSecretName(connectTrustSubleafName)
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
