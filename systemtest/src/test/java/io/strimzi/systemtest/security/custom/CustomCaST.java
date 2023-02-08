/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.custom;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.strimzi.api.kafka.model.CertificateAuthority;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.annotations.ParallelSuite;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.security.SystemTestCertHolder;
import io.strimzi.systemtest.security.SystemTestCertManager;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Provides test cases to verify custom key-pair (public key + private key) manipulation. For instance:
 *  1. Replacing `cluster` key-pair (f.e., ca.crt and ca.key) to invoke renewal process
 *  2. Replacing `clients` key-pair (f.e., user.crt and user.key) to invoke renewal process
 *
 * @see <a href="https://strimzi.io/docs/operators/in-development/configuring.html#installing-your-own-ca-certificates-str">Installing your own ca certificates</a>
 * @see <a href="https://strimzi.io/docs/operators/in-development/configuring.html#proc-replacing-your-own-private-keys-str">Replacing your own private keys</a>
 */
@Tag(REGRESSION)
@ParallelSuite
public class CustomCaST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(CustomCaST.class);
    private static final String STRIMZI_INTERMEDIATE_CA = "C=CZ, L=Prague, O=Strimzi, CN=StrimziIntermediateCA";

    @ParallelNamespaceTest
    void testReplacingCustomClusterKeyPairToInvokeRenewalProcess(ExtensionContext extensionContext) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        final TestStorage testStorage = new TestStorage(extensionContext);
        // 0. Generate root and intermediate certificate authority with cluster CA
        SystemTestCertHolder clusterCa =  new SystemTestCertHolder(
            "CN=" + extensionContext.getRequiredTestClass().getSimpleName() + "ClusterCA",
            KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()),
            KafkaResources.clusterCaKeySecretName(testStorage.getClusterName()));

        prepareTestCaWithBundleAndKafkaCluster(extensionContext, clusterCa, testStorage);

        // ------- public key part

        // 4. Update the Secret for the CA certificate.
        //  a) Edit the existing secret to add the new CA certificate and update the certificate generation annotation value.
        //  b) Rename the current CA certificate to retain it
        Secret clusterCaCertificateSecret = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()));
        final String oldCaCertName = clusterCa.retrieveOldCertificateName(clusterCaCertificateSecret, "ca.crt");

        // store the old cert
        clusterCaCertificateSecret.getData().put(oldCaCertName, clusterCaCertificateSecret.getData().get("ca.crt"));

        //  c) Encode your new CA certificate into base64.
        LOGGER.info("Generating a new custom 'Cluster certificate authority' with `Root` and `Intermediate` for Strimzi and PEM bundles.");
        clusterCa = new SystemTestCertHolder(
            "CN=" + extensionContext.getRequiredTestClass().getSimpleName() + "ClusterCAv2",
            KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()),
            KafkaResources.clusterCaKeySecretName(testStorage.getClusterName()));

        //  d) Update the CA certificate.
        clusterCaCertificateSecret.getData().put("ca.crt", Base64.getEncoder().encodeToString(Files.readAllBytes(Paths.get(clusterCa.getBundle().getCertPath()))));

        //  e) Increase the value of the CA certificate generation annotation.
        //  f) Save the secret with the new CA certificate and certificate generation annotation value.
        SystemTestCertHolder.increaseCertGenerationCounterInSecret(clusterCaCertificateSecret, testStorage, Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION);

        // ------- private key part

        // 5. Update the Secret for the CA key used to sign your new CA certificate.
        //  a) Edit the existing secret to add the new CA key and update the key generation annotation value.
        final Secret clusterCaKeySecret = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), KafkaResources.clusterCaKeySecretName(testStorage.getClusterName()));

        //  b) Encode the CA key into base64.
        //  c) Update the CA key.
        final File strimziKeyPKCS8 = SystemTestCertManager.convertPrivateKeyToPKCS8File(clusterCa.getSystemTestCa().getPrivateKey());
        clusterCaKeySecret.getData().put("ca.key", Base64.getEncoder().encodeToString(Files.readAllBytes(Paths.get(strimziKeyPKCS8.getAbsolutePath()))));

        // d) Increase the value of the CA key generation annotation.
        // 6. Save the secret with the new CA key and key generation annotation value.
        SystemTestCertHolder.increaseCertGenerationCounterInSecret(clusterCaKeySecret, testStorage, Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION);

        // --- verification phase (Rolling Update of components)

        // 7. save the current state of the Kafka, ZooKeeper and EntityOperator pods
        Map<String, String> kafkaPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getKafkaSelector());

        Map<String, String> zkPods = new HashMap<>();
        if (!Environment.isKRaftModeEnabled()) {
            zkPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getZookeeperSelector());
        }
        Map<String, String> eoPod = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), testStorage.getEoDeploymentName());

        // 8. Resume reconciliation from the pause.
        LOGGER.info("Resume the reconciliation of the Kafka custom resource ({}).", testStorage.getKafkaStatefulSetName());
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> {
            kafka.getMetadata().getAnnotations().remove(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION);
        }, testStorage.getNamespaceName());

        // 9. On the next reconciliation, the Cluster Operator performs a `rolling update`:
        //      a) ZooKeeper
        //      b) Kafka
        //      c) and other components to trust the new CA certificate. (i.e., EntityOperator)
        //  When the rolling update is complete, the Cluster Operator
        //  will start a new one to generate new server certificates signed by the new CA key.
        if (!Environment.isKRaftModeEnabled()) {
            zkPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getZookeeperSelector(), 3, zkPods);
        }
        kafkaPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), 3, kafkaPods);
        eoPod = DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), testStorage.getEoDeploymentName(), 1, eoPod);

        // second Rolling update to generate new server certificates signed by the new CA key.
        if (!Environment.isKRaftModeEnabled()) {
            RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getZookeeperSelector(), 3, zkPods);
        }
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), 3, kafkaPods);
        DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), testStorage.getEoDeploymentName(), 1, eoPod);

        // 10. Try to produce messages
        producerMessages(extensionContext, testStorage);

        // 12. Check that old certificate is not removed after RUs
        clusterCaCertificateSecret = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()));
        assertNotNull(clusterCaCertificateSecret.getData().get(oldCaCertName));
    }

    @ParallelNamespaceTest
    void testReplacingCustomClientsKeyPairToInvokeRenewalProcess(ExtensionContext extensionContext) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        final TestStorage testStorage = new TestStorage(extensionContext);
        // 0. Generate root and intermediate certificate authority with clients CA
        SystemTestCertHolder clientsCa = new SystemTestCertHolder("CN=" + extensionContext.getRequiredTestClass().getSimpleName() + "ClientsCA",
            KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName()),
            KafkaResources.clientsCaKeySecretName(testStorage.getClusterName()));

        prepareTestCaWithBundleAndKafkaCluster(extensionContext, clientsCa, testStorage);

        // ------- public key part

        // 4. Update the Secret for the CA certificate.
        //  a) Edit the existing secret to add the new CA certificate and update the certificate generation annotation value.
        //  b) Rename the current CA certificate to retain it
        final Secret clientsCaCertificateSecret = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName()));
        final String oldCaCertName = clientsCa.retrieveOldCertificateName(clientsCaCertificateSecret, "ca.crt");

        // store the old cert
        clientsCaCertificateSecret.getData().put(oldCaCertName, clientsCaCertificateSecret.getData().get("ca.crt"));

        //  c) Encode your new CA certificate into base64.
        LOGGER.info("Generating a new custom 'User certificate authority' with `Root` and `Intermediate` for Strimzi and PEM bundles.");
        clientsCa = new SystemTestCertHolder(
            "CN=" + extensionContext.getRequiredTestClass().getSimpleName() + "ClientsCAv2",
            KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName()),
            KafkaResources.clientsCaKeySecretName(testStorage.getClusterName()));

        //  d) Update the CA certificate.
        clientsCaCertificateSecret.getData().put("ca.crt", Base64.getEncoder().encodeToString(Files.readAllBytes(Paths.get(clientsCa.getBundle().getCertPath()))));

        //  e) Increase the value of the CA certificate generation annotation.
        //  f) Save the secret with the new CA certificate and certificate generation annotation value.
        SystemTestCertHolder.increaseCertGenerationCounterInSecret(clientsCaCertificateSecret, testStorage, Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION);

        // ------- private key part

        // 5. Update the Secret for the CA key used to sign your new CA certificate.
        //  a) Edit the existing secret to add the new CA key and update the key generation annotation value.
        final Secret clientsCaKeySecret = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), KafkaResources.clientsCaKeySecretName(testStorage.getClusterName()));

        //  b) Encode the CA key into base64.
        //  c) Update the CA key.
        final File strimziKeyPKCS8 = SystemTestCertManager.convertPrivateKeyToPKCS8File(clientsCa.getSystemTestCa().getPrivateKey());
        clientsCaKeySecret.getData().put("ca.key", Base64.getEncoder().encodeToString(Files.readAllBytes(Paths.get(strimziKeyPKCS8.getAbsolutePath()))));
        // d) Increase the value of the CA key generation annotation.
        // 6. Save the secret with the new CA key and key generation annotation value.
        SystemTestCertHolder.increaseCertGenerationCounterInSecret(clientsCaKeySecret, testStorage, Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION);

        // --- verification phase (Rolling Update of components)

        // 7. save the current state of the Kafka, ZooKeeper and EntityOperator pods
        final Map<String, String> kafkaPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getKafkaSelector());

        Map<String, String> zkPods = new HashMap<>();
        if (!Environment.isKRaftModeEnabled()) {
            zkPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getZookeeperSelector());
        }
        final Map<String, String> eoPod = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), testStorage.getEoDeploymentName());

        // 8. Resume reconciliation from the pause.
        LOGGER.info("Resume the reconciliation of the Kafka custom resource ({}).", testStorage.getKafkaStatefulSetName());
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> {
            kafka.getMetadata().getAnnotations().remove(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION);
        }, testStorage.getNamespaceName());

        // 9. On the next reconciliation, the Cluster Operator performs a `rolling update` only for the
        // `Kafka pods`. When the rolling update is complete, the Cluster Operator will start a new one to
        // generate new server certificates signed by the new CA key.

        // a) ZooKeeper must not roll
        if (!Environment.isKRaftModeEnabled()) {
            RollingUpdateUtils.waitForNoRollingUpdate(testStorage.getNamespaceName(), testStorage.getZookeeperSelector(), zkPods);
            assertThat(RollingUpdateUtils.componentHasRolled(testStorage.getNamespaceName(), testStorage.getZookeeperSelector(), zkPods), is(Boolean.FALSE));
        }

        // b) Kafka has to roll
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), 3, kafkaPods);

        // c) EO must not roll
        DeploymentUtils.waitForNoRollingUpdate(testStorage.getNamespaceName(), testStorage.getEoDeploymentName(), eoPod);

        // 10. Try to produce messages
        producerMessages(extensionContext, testStorage);
    }

    /**
     * Provides preparation for {@link #testReplacingCustomClientsKeyPairToInvokeRenewalProcess(ExtensionContext)} and
     * {@link #testReplacingCustomClusterKeyPairToInvokeRenewalProcess(ExtensionContext)} test cases. This consists of
     * creation of CA with bundles, deployment of Kafka cluster and eventually pausing the reconciliation for specific
     * Kafka cluster to proceed with updating public or private keys.
     *
     * @param extensionContext              context for test case
     * @param certificateAuthority          certificate authority of Clients or Cluster
     * @param testStorage                            auxiliary resources for test case
     */
    private void prepareTestCaWithBundleAndKafkaCluster(final ExtensionContext extensionContext, final SystemTestCertHolder certificateAuthority, final TestStorage testStorage) {
        // 1. Prepare correspondent Secrets from generated custom CA certificates
        //  a) Cluster or Clients CA
        certificateAuthority.prepareCustomSecretsFromBundles(testStorage.getNamespaceName(), testStorage.getClusterName());

        //  b) if Cluster CA is under test - we generate custom Clients CA (it's because in our Kafka configuration we
        //     specify to not generate CA automatically. Thus we need it generate on our own to avoid issue
        //     (f.e., Clients CA should not be generated, but the secrets were not found.)
        if (certificateAuthority.getCaCertSecretName().equals(KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName())) &&
            certificateAuthority.getCaKeySecretName().equals(KafkaResources.clusterCaKeySecretName(testStorage.getClusterName()))) {
            final SystemTestCertHolder clientsCa = new SystemTestCertHolder(
                "CN=" + extensionContext.getRequiredTestClass().getSimpleName() + "ClientsCA",
                KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName()),
                KafkaResources.clientsCaKeySecretName(testStorage.getClusterName()));
            clientsCa.prepareCustomSecretsFromBundles(testStorage.getNamespaceName(), testStorage.getClusterName());
        } else {
            // otherwise we generate Cluster CA
            final SystemTestCertHolder clusterCa = new SystemTestCertHolder(
                "CN=" + extensionContext.getRequiredTestClass().getSimpleName() + "ClusterCA",
                KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()),
                KafkaResources.clusterCaKeySecretName(testStorage.getClusterName()));
            clusterCa.prepareCustomSecretsFromBundles(testStorage.getNamespaceName(), testStorage.getClusterName());
        }

        // 2. Create a Kafka cluster without implicit generation of CA
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3)
            .editOrNewSpec()
                .withNewClientsCa()
                    .withRenewalDays(5)
                    .withValidityDays(20)
                    .withGenerateCertificateAuthority(false)
                .endClientsCa()
             .withNewClusterCa()
                    .withRenewalDays(5)
                    .withValidityDays(20)
                    .withGenerateCertificateAuthority(false)
                .endClusterCa()
            .endSpec()
            .build());

        // 3. Pause the reconciliation of the Kafka custom resource
        LOGGER.info("Pause the reconciliation of the Kafka custom resource ({}).", KafkaResources.kafkaStatefulSetName(testStorage.getClusterName()));
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> {
            Map<String, String> kafkaAnnotations = kafka.getMetadata().getAnnotations();
            if (kafkaAnnotations == null) {
                kafkaAnnotations = new HashMap<>();
            }
            // adding pause annotation
            kafkaAnnotations.put(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true");
            kafka.getMetadata().setAnnotations(kafkaAnnotations);
        }, testStorage.getNamespaceName());
    }

    private void producerMessages(final ExtensionContext extensionContext, final TestStorage testStorage) {
        // 11. Try to produce messages
        final KafkaClients kafkaBasicClientJob = new KafkaClientsBuilder()
            .withNamespaceName(testStorage.getNamespaceName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getClusterName())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withTopicName(testStorage.getTopicName())
            .withUserName(testStorage.getUserName())
            .withMessageCount(testStorage.getMessageCount())
            .withDelayMs(10)
            .build();

        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(testStorage).build());
        resourceManager.createResource(extensionContext, kafkaBasicClientJob.producerTlsStrimzi(testStorage.getClusterName()));

        ClientUtils.waitForClientSuccess(testStorage.getProducerName(), testStorage.getNamespaceName(), testStorage.getMessageCount());
    }

    @ParallelNamespaceTest
    void testCustomClusterCaAndClientsCaCertificates(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        final String testSuite = extensionContext.getRequiredTestClass().getSimpleName();

        final SystemTestCertHolder clientsCa = new SystemTestCertHolder(
            "CN=" + testSuite + "ClientsCA",
            KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName()),
            KafkaResources.clientsCaKeySecretName(testStorage.getClusterName()));
        final SystemTestCertHolder clusterCa = new SystemTestCertHolder(
            "CN=" + testSuite + "ClusterCA",
            KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()),
            KafkaResources.clusterCaKeySecretName(testStorage.getClusterName()));

        // prepare custom Ca and copy that to the related Secrets
        clientsCa.prepareCustomSecretsFromBundles(testStorage.getNamespaceName(), testStorage.getClusterName());
        clusterCa.prepareCustomSecretsFromBundles(testStorage.getNamespaceName(), testStorage.getClusterName());

        final X509Certificate clientsCert = SecretUtils.getCertificateFromSecret(kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(),
            KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName())), "ca.crt");
        final X509Certificate clusterCert = SecretUtils.getCertificateFromSecret(kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(),
            KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName())), "ca.crt");

        checkCustomCaCorrectness(clientsCa, clientsCert);
        checkCustomCaCorrectness(clusterCa, clusterCert);

        LOGGER.info("Deploy kafka with new certs/secrets.");
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3, 3)
            .editSpec()
                .withNewClusterCa()
                    .withGenerateCertificateAuthority(false)
                .endClusterCa()
                .withNewClientsCa()
                    .withGenerateCertificateAuthority(false)
                .endClientsCa()
            .endSpec()
            .build()
        );

        LOGGER.info("Check Kafka(s) and Zookeeper(s) certificates.");
        final X509Certificate kafkaCert = SecretUtils.getCertificateFromSecret(kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(),
            testStorage.getClusterName() + "-kafka-brokers"), testStorage.getClusterName() + "-kafka-0.crt");
        assertThat("KafkaCert does not have expected test Issuer: " + kafkaCert.getIssuerDN(),
                SystemTestCertManager.containsAllDN(kafkaCert.getIssuerX500Principal().getName(), clusterCa.getSubjectDn()));

        if (!Environment.isKRaftModeEnabled()) {
            X509Certificate zookeeperCert = SecretUtils.getCertificateFromSecret(kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(),
                    testStorage.getClusterName() + "-zookeeper-nodes"), testStorage.getClusterName() + "-zookeeper-0.crt");
            assertThat("ZookeeperCert does not have expected test Subject: " + zookeeperCert.getIssuerDN(),
                    SystemTestCertManager.containsAllDN(zookeeperCert.getIssuerX500Principal().getName(), clusterCa.getSubjectDn()));
        }

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName()).build());

        LOGGER.info("Check KafkaUser certificate.");
        final KafkaUser user = KafkaUserTemplates.tlsUser(testStorage).build();
        resourceManager.createResource(extensionContext, user);
        final X509Certificate userCert = SecretUtils.getCertificateFromSecret(kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(),
            testStorage.getUserName()), "user.crt");
        assertThat("Generated ClientsCA does not have expected test Subject: " + userCert.getIssuerDN(),
                SystemTestCertManager.containsAllDN(userCert.getIssuerX500Principal().getName(), clientsCa.getSubjectDn()));

        LOGGER.info("Send and receive messages over TLS.");

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withMessageCount(testStorage.getMessageCount())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withTopicName(testStorage.getTopicName())
            .withUserName(testStorage.getUserName())
            .build();

        LOGGER.info("Checking produced and consumed messages via TLS");
        resourceManager.createResource(extensionContext, kafkaClients.producerTlsStrimzi(testStorage.getClusterName()), kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientsSuccess(testStorage, false);
    }

    private boolean checkMountVolumeSecret(final String namespaceName, final String podName, final VolumeMount volumeMount,
                                           final String principalDNType, final String expectedPrincipal) {
        final String dn = cmdKubeClient(namespaceName).execInPod(podName, "/bin/bash", "-c",
            "openssl x509 -in " + volumeMount.getMountPath() + "/ca.crt -noout -nameopt RFC2253 -" + principalDNType).out().strip();
        final String certOutIssuer = dn.substring(principalDNType.length() + 1).replace("/", ",");
        return SystemTestCertManager.containsAllDN(certOutIssuer, expectedPrincipal);
    }

    private void checkCustomCaCorrectness(final SystemTestCertHolder caHolder, final X509Certificate certificate) {
        LOGGER.info("Check ClusterCA and ClientsCA certificates.");
        assertThat("Generated ClientsCA or ClusterCA does not have expected Issuer: " + certificate.getIssuerDN(),
            SystemTestCertManager.containsAllDN(certificate.getIssuerX500Principal().getName(), STRIMZI_INTERMEDIATE_CA));
        assertThat("Generated ClientsCA or ClusterCA does not have expected Subject: " + certificate.getSubjectDN(),
            SystemTestCertManager.containsAllDN(certificate.getSubjectX500Principal().getName(), caHolder.getSubjectDn()));
    }

    @ParallelNamespaceTest
    void testCustomClusterCACertificateRenew(ExtensionContext extensionContext) {
        TestStorage testStorage = new TestStorage(extensionContext);
        final String testSuite = extensionContext.getRequiredTestClass().getSimpleName();

        final SystemTestCertHolder clusterCa = new SystemTestCertHolder(
            "CN=" + testSuite + "ClusterCA",
            KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()),
            KafkaResources.clusterCaKeySecretName(testStorage.getClusterName()));

        // prepare custom Ca and copy that to the related Secrets
        clusterCa.prepareCustomSecretsFromBundles(testStorage.getNamespaceName(), testStorage.getClusterName());

        final X509Certificate clusterCert = SecretUtils.getCertificateFromSecret(kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(),
            KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName())), "ca.crt");

        checkCustomCaCorrectness(clusterCa, clusterCert);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
            .editOrNewSpec()
                // Note: Clients Ca is generated automatically
                .withNewClusterCa()
                    .withRenewalDays(15)
                    .withValidityDays(20)
                    .withGenerateCertificateAuthority(false)
                .endClusterCa()
            .endSpec()
            .build());

        final Map<String, String> zkPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getZookeeperSelector());
        final Map<String, String> kafkaPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getKafkaSelector());
        final Map<String, String> eoPod = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), testStorage.getEoDeploymentName());

        Secret clusterCASecret = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()));
        X509Certificate cacert = SecretUtils.getCertificateFromSecret(clusterCASecret, "ca.crt");
        final Date initialCertStartTime = cacert.getNotBefore();
        final Date initialCertEndTime = cacert.getNotAfter();

        // Check Broker kafka certificate dates
        Secret brokerCertCreationSecret = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), testStorage.getClusterName() + "-kafka-brokers");
        X509Certificate kafkaBrokerCert = SecretUtils.getCertificateFromSecret(brokerCertCreationSecret, testStorage.getClusterName() + "-kafka-0.crt");
        final Date initialKafkaBrokerCertStartTime = kafkaBrokerCert.getNotBefore();
        final Date initialKafkaBrokerCertEndTime = kafkaBrokerCert.getNotAfter();

        // Check Zookeeper certificate dates
        Secret zkCertCreationSecret;
        X509Certificate zkBrokerCert;
        Date initialZkCertStartTime = null;
        Date initialZkCertEndTime = null;
        if (!Environment.isKRaftModeEnabled()) {
            zkCertCreationSecret = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), testStorage.getClusterName() + "-zookeeper-nodes");
            zkBrokerCert = SecretUtils.getCertificateFromSecret(zkCertCreationSecret, testStorage.getClusterName() + "-zookeeper-0.crt");
            initialZkCertStartTime = zkBrokerCert.getNotBefore();
            initialZkCertEndTime = zkBrokerCert.getNotAfter();
        }

        LOGGER.info("Change of kafka validity and renewal days - reconciliation should start.");
        final CertificateAuthority newClusterCA = new CertificateAuthority();
        newClusterCA.setRenewalDays(150);
        newClusterCA.setValidityDays(200);
        newClusterCA.setGenerateCertificateAuthority(false);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> k.getSpec().setClusterCa(newClusterCA), testStorage.getNamespaceName());

        // On the next reconciliation, the Cluster Operator performs a `rolling update`:
        //   a) ZooKeeper
        //   b) Kafka
        //   c) and other components to trust the new Cluster CA certificate. (i.e., EntityOperator)
        if (!Environment.isKRaftModeEnabled()) {
            RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getZookeeperSelector(), 3, zkPods);
        }
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), 3, kafkaPods);
        DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), testStorage.getEoDeploymentName(), 1, eoPod);

        // Read renewed secret/certs again
        clusterCASecret = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()));
        cacert = SecretUtils.getCertificateFromSecret(clusterCASecret, "ca.crt");
        final Date changedCertStartTime = cacert.getNotBefore();
        final Date changedCertEndTime = cacert.getNotAfter();

        // Check renewed Broker kafka certificate dates
        brokerCertCreationSecret = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), testStorage.getClusterName() + "-kafka-brokers");
        kafkaBrokerCert = SecretUtils.getCertificateFromSecret(brokerCertCreationSecret, testStorage.getClusterName() + "-kafka-0.crt");
        final Date changedKafkaBrokerCertStartTime = kafkaBrokerCert.getNotBefore();
        final Date changedKafkaBrokerCertEndTime = kafkaBrokerCert.getNotAfter();

        // Check renewed Zookeeper certificate dates
        Date changedZkCertStartTime = null;
        Date changedZkCertEndTime = null;
        if (!Environment.isKRaftModeEnabled()) {
            zkCertCreationSecret = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), testStorage.getClusterName() + "-zookeeper-nodes");
            zkBrokerCert = SecretUtils.getCertificateFromSecret(zkCertCreationSecret, testStorage.getClusterName() + "-zookeeper-0.crt");
            changedZkCertStartTime = zkBrokerCert.getNotBefore();
            changedZkCertEndTime = zkBrokerCert.getNotAfter();
        }

        LOGGER.info("Initial ClusterCA cert dates: " + initialCertStartTime + " --> " + initialCertEndTime);
        LOGGER.info("Changed ClusterCA cert dates: " + changedCertStartTime + " --> " + changedCertEndTime);
        LOGGER.info("KafkaBroker cert creation dates: " + initialKafkaBrokerCertStartTime + " --> " + initialKafkaBrokerCertEndTime);
        LOGGER.info("KafkaBroker cert changed dates:  " + changedKafkaBrokerCertStartTime + " --> " + changedKafkaBrokerCertEndTime);
        if (!Environment.isKRaftModeEnabled()) {
            LOGGER.info("Zookeeper cert creation dates: " + initialZkCertStartTime + " --> " + initialZkCertEndTime);
            LOGGER.info("Zookeeper cert changed dates:  " + changedZkCertStartTime + " --> " + changedZkCertEndTime);
        }

        assertThat("ClusterCA cert should not have changed.",
            initialCertEndTime.compareTo(changedCertEndTime) == 0);
        assertThat("Broker certificates start dates have not been renewed.",
            initialKafkaBrokerCertStartTime.compareTo(changedKafkaBrokerCertStartTime) < 0);
        assertThat("Broker certificates end dates have not been renewed.",
            initialKafkaBrokerCertEndTime.compareTo(changedKafkaBrokerCertEndTime) < 0);
        if (!Environment.isKRaftModeEnabled()) {
            assertThat("Zookeeper certificates start dates have not been renewed.",
                    initialZkCertStartTime.compareTo(changedZkCertStartTime) < 0);
            assertThat("Zookeeper certificates end dates have not been renewed.",
                    initialZkCertEndTime.compareTo(changedZkCertEndTime) < 0);
        }
    }

    @ParallelNamespaceTest
    void testClientsCaCertificateRenew(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        final String testSuite = extensionContext.getRequiredTestClass().getSimpleName();

        final SystemTestCertHolder clientsCa = new SystemTestCertHolder(
            "CN=" + testSuite + "ClientsCA",
            KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName()),
            KafkaResources.clientsCaKeySecretName(testStorage.getClusterName()));

        // prepare custom Ca and copy that to the related Secrets
        clientsCa.prepareCustomSecretsFromBundles(testStorage.getNamespaceName(), testStorage.getClusterName());

        final X509Certificate clientsCert = SecretUtils.getCertificateFromSecret(kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(),
            KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName())), "ca.crt");

        checkCustomCaCorrectness(clientsCa, clientsCert);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
            .editOrNewSpec()
                // Note: Cluster Ca is generated automatically
                .withNewClientsCa()
                    .withRenewalDays(15)
                    .withValidityDays(20)
                    .withGenerateCertificateAuthority(false)
                .endClientsCa()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(testStorage).build());
        final Map<String, String> entityPods = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(testStorage.getClusterName()));

        // Check initial clientsCA validity days
        Secret clientsCASecret = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName()));
        X509Certificate cacert = SecretUtils.getCertificateFromSecret(clientsCASecret, "ca.crt");
        final Date initialCertStartTime = cacert.getNotBefore();
        final Date initialCertEndTime = cacert.getNotAfter();

        // Check initial kafkauser validity days
        X509Certificate userCert = SecretUtils.getCertificateFromSecret(kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), testStorage.getUserName()), "user.crt");
        final Date initialKafkaUserCertStartTime = userCert.getNotBefore();
        final Date initialKafkaUserCertEndTime = userCert.getNotAfter();

        LOGGER.info("Change of kafka validity and renewal days - reconciliation should start.");
        final CertificateAuthority newClientsCA = new CertificateAuthority();
        newClientsCA.setRenewalDays(150);
        newClientsCA.setValidityDays(200);
        newClientsCA.setGenerateCertificateAuthority(false);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> k.getSpec().setClientsCa(newClientsCA), testStorage.getNamespaceName());

        // Wait for reconciliation and verify certs have been updated
        DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), testStorage.getEoDeploymentName(), 1, entityPods);

        // Read renewed secret/certs again
        clientsCASecret = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName()));
        cacert = SecretUtils.getCertificateFromSecret(clientsCASecret, "ca.crt");
        final Date changedCertStartTime = cacert.getNotBefore();
        final Date changedCertEndTime = cacert.getNotAfter();

        userCert = SecretUtils.getCertificateFromSecret(kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), testStorage.getUserName()), "user.crt");
        final Date changedKafkaUserCertStartTime = userCert.getNotBefore();
        final Date changedKafkaUserCertEndTime = userCert.getNotAfter();

        LOGGER.info("Initial ClientsCA cert dates: " + initialCertStartTime + " --> " + initialCertEndTime);
        LOGGER.info("Changed ClientsCA cert dates: " + changedCertStartTime + " --> " + changedCertEndTime);
        LOGGER.info("Initial userCert dates: " + initialKafkaUserCertStartTime + " --> " + initialKafkaUserCertEndTime);
        LOGGER.info("Changed userCert dates: " + changedKafkaUserCertStartTime + " --> " + changedKafkaUserCertEndTime);

        assertThat("ClientsCA cert should not have changed.",
                initialCertEndTime.compareTo(changedCertEndTime) == 0);
        assertThat("UserCert start date has been renewed",
                initialKafkaUserCertStartTime.compareTo(changedKafkaUserCertStartTime) < 0);
        assertThat("UserCert end date has been renewed",
                initialKafkaUserCertEndTime.compareTo(changedKafkaUserCertEndTime) < 0);
    }

}
