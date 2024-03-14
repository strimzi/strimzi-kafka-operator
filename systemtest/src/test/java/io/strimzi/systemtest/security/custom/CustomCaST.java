/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.custom;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.common.CertificateAuthority;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.StrimziPodSetResource;
import io.strimzi.systemtest.security.SystemTestCertHolder;
import io.strimzi.systemtest.security.SystemTestCertManager;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StrimziPodSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

import static io.strimzi.systemtest.TestConstants.REGRESSION;
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
public class CustomCaST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(CustomCaST.class);
    private static final String STRIMZI_INTERMEDIATE_CA = "C=CZ, L=Prague, O=Strimzi, CN=StrimziIntermediateCA";

    /** This test focuses on manual renewal of custom cluster CA.
     * Steps are following. Create own cluster CA. From the CA create a Bundle that will be used in kafka cluster which is then deployed.
     * Once everything is set up, kafka reconciliation is paused using annotation. Then a newly generated cluster CA keypair is put in place.
     * When annotation is removed, kafka resumes and tries to renew cluster certificates using the new CA keypair.
     * Note: There is a need to keep an old CA key in the secret and remove it only after all components successfully
     * roll several times (due to the fact that it takes multiple rolling updates to finally update from old keypair to the new one).
     * Test also verifies communication by producing and consuming messages.
     */
    @ParallelNamespaceTest
    void testReplacingCustomClusterKeyPairToInvokeRenewalProcess() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        // Generate root and intermediate certificate authority with cluster CA
        SystemTestCertHolder clusterCa = new SystemTestCertHolder(
            "CN=" + testStorage.getTestName() + "ClusterCA",
            KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()),
            KafkaResources.clusterCaKeySecretName(testStorage.getClusterName()));

        prepareTestCaWithBundleAndKafkaCluster(clusterCa, testStorage, 5, 20);

        // Take snapshots for later comparison
        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        Map<String, String> controllerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());
        Map<String, String> eoPod = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), testStorage.getEoDeploymentName());

        // Create new CA and renew old one with it
        final SystemTestCertHolder newClusterCa = new SystemTestCertHolder(
            "CN=" + testStorage.getTestName() + "ClusterCAv2",
            KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()),
            KafkaResources.clusterCaKeySecretName(testStorage.getClusterName()));

        // Get old certificate ca.crt field name
        Secret clusterCaCertificateSecret = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()));
        final String oldCaCertName = clusterCa.retrieveOldCertificateName(clusterCaCertificateSecret, "ca.crt");

        manuallyRenewCa(testStorage, clusterCa, newClusterCa);

        // On the next reconciliation, the Cluster Operator performs a `rolling update`:
        //      a) ZooKeeper
        //      b) Kafka
        //      c) and other components to trust the new CA certificate. (i.e., Entity Operator)
        // When the rolling update is complete, the Cluster Operator
        // will start a new one to generate new server certificates signed by the new CA key.
        LOGGER.info("Waiting for first round of rolling update");
        controllerPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getControllerSelector(), 3, controllerPods);
        brokerPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);
        eoPod = DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), testStorage.getEoDeploymentName(), 1, eoPod);

        // Second Rolling update to generate new server certificates signed by the new CA key.
        LOGGER.info("Waiting for second round of rolling update");
        controllerPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getControllerSelector(), 3, controllerPods);
        brokerPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);
        eoPod = DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), testStorage.getEoDeploymentName(), 1, eoPod);

        LOGGER.info("All rounds of rolling update have been finished");
        // Try to produce messages
        final KafkaClients kafkaBasicClientJob = ClientUtils.getInstantTlsClients(testStorage);
        resourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage).build());
        resourceManager.createResourceWithWait(kafkaBasicClientJob.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientSuccess(testStorage.getProducerName(), testStorage.getNamespaceName(), testStorage.getMessageCount());

        // Get new certificate secret and assert old value is still present
        clusterCaCertificateSecret = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()));
        assertNotNull(clusterCaCertificateSecret.getData().get(oldCaCertName));

        // Remove outdated certificate from the secret configuration to ensure that the cluster no longer trusts them.
        clusterCaCertificateSecret.getData().remove(oldCaCertName);
        kubeClient().patchSecret(testStorage.getNamespaceName(), clusterCaCertificateSecret.getMetadata().getName(), clusterCaCertificateSecret);

        // Start a manual rolling update of your cluster to pick up the changes made to the secret configuration.
        StrimziPodSetUtils.annotateStrimziPodSet(testStorage.getNamespaceName(), testStorage.getControllerComponentName(), Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true"));
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getControllerSelector(), 3, controllerPods);

        StrimziPodSetUtils.annotateStrimziPodSet(testStorage.getNamespaceName(), testStorage.getBrokerComponentName(), Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true"));
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);

        DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), testStorage.getEoDeploymentName(), 1, eoPod);
    }

    /** This test focuses on manual renewal of custom clients CA.
     * Steps are following. Create own cluster CA. From the CA create a Bundle that will be used in kafka cluster which is then deployed.
     * Once everything is set up, kafka reconciliation is paused using annotation. Then a newly generated cluster CA keypair is put in place.
     * When annotation is removed, kafka resumes and tries to renew clients certificates using the new CA keypair.
     * Note: There is a need to keep an old CA key in the secret and remove it only after all components successfully
     * roll several times (due to the fact that it takes multiple rolling updates to finally update from old keypair to the new one).
     * Because this test specifically targets clients certificates, EO and ZK must NOT roll, only broker pods should.
     * Test also verifies communication by producing and consuming messages.
     */
    @ParallelNamespaceTest
    void testReplacingCustomClientsKeyPairToInvokeRenewalProcess() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        // Generate root and intermediate certificate authority with clients CA
        final SystemTestCertHolder clientsCa = new SystemTestCertHolder(
            "CN=" + testStorage.getTestName() + "ClientsCA",
            KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName()),
            KafkaResources.clientsCaKeySecretName(testStorage.getClusterName()));

        prepareTestCaWithBundleAndKafkaCluster(clientsCa, testStorage, 5, 20);

        // Take snapshots for later comparison
        final Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        final Map<String, String> controllerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());
        final Map<String, String> eoPod = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), testStorage.getEoDeploymentName());

        LOGGER.info("Generating a new custom 'User certificate authority' with `Root` and `Intermediate` for Strimzi and PEM bundles");
        final SystemTestCertHolder newClientsCa = new SystemTestCertHolder(
            "CN=" + testStorage.getTestName() + "ClientsCAv2",
            KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName()),
            KafkaResources.clientsCaKeySecretName(testStorage.getClusterName()));

        manuallyRenewCa(testStorage, clientsCa, newClientsCa);

        // On the next reconciliation, the Cluster Operator performs a `rolling update` only for the
        // `Kafka pods`. When the rolling update is complete, the Cluster Operator will start a new one to
        // generate new server certificates signed by the new CA key.

        // Zookeper must not roll
        if (!Environment.isKRaftModeEnabled()) {
            RollingUpdateUtils.waitForNoRollingUpdate(testStorage.getNamespaceName(), testStorage.getControllerSelector(), controllerPods);
            assertThat(RollingUpdateUtils.componentHasRolled(testStorage.getNamespaceName(), testStorage.getControllerSelector(), controllerPods), is(Boolean.FALSE));
        }

        // Kafka has to roll
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);

        // EO must not roll
        DeploymentUtils.waitForNoRollingUpdate(testStorage.getNamespaceName(), testStorage.getEoDeploymentName(), eoPod);

        // Try to produce messages
        final KafkaClients kafkaBasicClientJob = ClientUtils.getInstantTlsClients(testStorage);
        resourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage).build());
        resourceManager.createResourceWithWait(kafkaBasicClientJob.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientSuccess(testStorage.getProducerName(), testStorage.getNamespaceName(), testStorage.getMessageCount());
    }

    /** This tests focuses on verifying the functionality of custom cluster and clients CAs.
     * Steps are following. Before deploying kafka a clients and cluster CAs are created and deployed as secrets.
     * Kafka is then deployed with those, forcing it NOT to generate own certificate authority.
     * After verification of correct certificates on zookeeper, user certificates are checked for correctness as well.
     * Then a producer / consumer jos are deployed to verify communication.
     */
    @ParallelNamespaceTest
    void testCustomClusterCaAndClientsCaCertificates() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        final SystemTestCertHolder clientsCa = new SystemTestCertHolder(
            "CN=" + testStorage.getTestName() + "ClientsCA",
            KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName()),
            KafkaResources.clientsCaKeySecretName(testStorage.getClusterName()));
        final SystemTestCertHolder clusterCa = new SystemTestCertHolder(
            "CN=" + testStorage.getTestName() + "ClusterCA",
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

        LOGGER.info("Deploying Kafka with new certs, Secrets");
        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3, 3)
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

        LOGGER.info("Check Kafka(s) and ZooKeeper(s) certificates");
        String brokerPodName = kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector()).get(0).getMetadata().getName();
        final X509Certificate kafkaCert = SecretUtils.getCertificateFromSecret(kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(),
            testStorage.getClusterName() + "-kafka-brokers"), brokerPodName + ".crt");
        assertThat("KafkaCert does not have expected test Issuer: " + kafkaCert.getIssuerDN(),
                SystemTestCertManager.containsAllDN(kafkaCert.getIssuerX500Principal().getName(), clusterCa.getSubjectDn()));

        if (!Environment.isKRaftModeEnabled()) {
            X509Certificate zookeeperCert = SecretUtils.getCertificateFromSecret(kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(),
                    testStorage.getClusterName() + "-zookeeper-nodes"), testStorage.getClusterName() + "-zookeeper-0.crt");
            assertThat("ZookeeperCert does not have expected test Subject: " + zookeeperCert.getIssuerDN(),
                    SystemTestCertManager.containsAllDN(zookeeperCert.getIssuerX500Principal().getName(), clusterCa.getSubjectDn()));
        }

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());

        LOGGER.info("Check KafkaUser certificate");
        final KafkaUser user = KafkaUserTemplates.tlsUser(testStorage).build();
        resourceManager.createResourceWithWait(user);
        final X509Certificate userCert = SecretUtils.getCertificateFromSecret(kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(),
            testStorage.getUsername()), "user.crt");
        assertThat("Generated ClientsCA does not have expected test Subject: " + userCert.getIssuerDN(),
                SystemTestCertManager.containsAllDN(userCert.getIssuerX500Principal().getName(), clientsCa.getSubjectDn()));

        LOGGER.info("Send and receive messages over TLS");
        final KafkaClients kafkaClients = ClientUtils.getInstantTlsClients(testStorage);
        resourceManager.createResourceWithWait(kafkaClients.producerTlsStrimzi(testStorage.getClusterName()), kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantClientSuccess(testStorage);
    }

    /** This tests focuses on invoking certificate renewal without renewing the ClusterCA.
     * Steps are following. Create own cluster CA. From the CA create a Bundle that will be used in kafka cluster which is then deployed.
     * Once everything is set up, kafka reconciliation is paused using annotation. Then new validityDays and renewalDays are set for cluster CA.
     * When annotation is removed, kafka resumes and tries to renew cluster certificates using the CA with different validityDays and renewalDays.
     * Test also verifies that the CA has not been renewed, only the certificates provided by the CA.
     */
    @ParallelNamespaceTest
    void testReplaceCustomClusterCACertificateValidityToInvokeRenewalProcess() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final int renewalDays = 15;
        final int newRenewalDays = 150;
        final int validityDays = 20;
        final int newValidityDays = 200;

        SystemTestCertHolder clusterCa = new SystemTestCertHolder(
            "CN=" + testStorage.getTestName() + "ClusterCA",
            KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()),
            KafkaResources.clusterCaKeySecretName(testStorage.getClusterName()));

        prepareTestCaWithBundleAndKafkaCluster(clusterCa, testStorage, renewalDays, validityDays);

        // Make snapshots with original CA
        final Map<String, String> controllerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());
        final Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        final Map<String, String> eoPod = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), testStorage.getEoDeploymentName());

        Secret clusterCASecret = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()));
        X509Certificate cacert = SecretUtils.getCertificateFromSecret(clusterCASecret, "ca.crt");
        final Date initialCertStartTime = cacert.getNotBefore();
        final Date initialCertEndTime = cacert.getNotAfter();

        // Check Broker kafka certificate dates
        String brokerPodName = kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector()).get(0).getMetadata().getName();
        Secret brokerCertCreationSecret = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), testStorage.getClusterName() + "-kafka-brokers");
        X509Certificate kafkaBrokerCert = SecretUtils.getCertificateFromSecret(brokerCertCreationSecret, brokerPodName + ".crt");
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

        // Pause Kafka reconciliation
        LOGGER.info("Pause the reconciliation of the Kafka custom resource ({})", StrimziPodSetResource.getBrokerComponentName(testStorage.getClusterName()));
        KafkaUtils.annotateKafka(testStorage.getClusterName(), testStorage.getNamespaceName(), Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true"));

        // To test trigger of renewal of CA with short validity dates, both new dates need to be set
        // Otherwise we apply completely new CA using retained old certificate to change manually

        LOGGER.info("Change of Kafka validity and renewal days - reconciliation should start");
        final CertificateAuthority newClusterCA = new CertificateAuthority();
        newClusterCA.setRenewalDays(newRenewalDays);
        newClusterCA.setValidityDays(newValidityDays);
        newClusterCA.setGenerateCertificateAuthority(false);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> k.getSpec().setClusterCa(newClusterCA), testStorage.getNamespaceName());

        // Resume Kafka reconciliation
        LOGGER.info("Resume the reconciliation of the Kafka custom resource ({})", StrimziPodSetResource.getBrokerComponentName(testStorage.getClusterName()));
        KafkaUtils.removeAnnotation(testStorage.getClusterName(), testStorage.getNamespaceName(), Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION);

        // On the next reconciliation, the Cluster Operator performs a `rolling update`:
        //   a) ZooKeeper
        //   b) Kafka
        //   c) and other components to trust the new Cluster CA certificate. (i.e., Entity Operator)
        if (!Environment.isKRaftModeEnabled()) {
            RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getControllerSelector(), 3, controllerPods);
        }
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);
        DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), testStorage.getEoDeploymentName(), 1, eoPod);

        // Read renewed secret/certs again
        clusterCASecret = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()));
        cacert = SecretUtils.getCertificateFromSecret(clusterCASecret, "ca.crt");
        final Date changedCertStartTime = cacert.getNotBefore();
        final Date changedCertEndTime = cacert.getNotAfter();

        // Check renewed Broker kafka certificate dates
        brokerCertCreationSecret = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), testStorage.getClusterName() + "-kafka-brokers");
        kafkaBrokerCert = SecretUtils.getCertificateFromSecret(brokerCertCreationSecret, brokerPodName + ".crt");
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

        // Print out certificate dates for debug
        LOGGER.info("Initial ClusterCA cert dates: " + initialCertStartTime + " --> " + initialCertEndTime);
        LOGGER.info("Changed ClusterCA cert dates: " + changedCertStartTime + " --> " + changedCertEndTime);
        LOGGER.info("Kafka Broker cert creation dates: " + initialKafkaBrokerCertStartTime + " --> " + initialKafkaBrokerCertEndTime);
        LOGGER.info("Kafka Broker cert changed dates:  " + changedKafkaBrokerCertStartTime + " --> " + changedKafkaBrokerCertEndTime);
        if (!Environment.isKRaftModeEnabled()) {
            LOGGER.info("ZooKeeper cert creation dates: " + initialZkCertStartTime + " --> " + initialZkCertEndTime);
            LOGGER.info("ZooKeeper cert changed dates:  " + changedZkCertStartTime + " --> " + changedZkCertEndTime);
        }

        // Verify renewal result
        assertThat("ClusterCA cert should not have changed start date.", initialCertEndTime.compareTo(changedCertEndTime) == 0);
        assertThat("Broker certificates start dates should have been renewed.", initialKafkaBrokerCertStartTime.compareTo(changedKafkaBrokerCertStartTime) < 0);
        assertThat("Broker certificates end dates should have been renewed.", initialKafkaBrokerCertEndTime.compareTo(changedKafkaBrokerCertEndTime) < 0);
        if (!Environment.isKRaftModeEnabled()) {
            assertThat("Zookeeper certificates start dates should have been renewed.", initialZkCertStartTime.compareTo(changedZkCertStartTime) < 0);
            assertThat("Zookeeper certificates end dates should have been renewed.", initialZkCertEndTime.compareTo(changedZkCertEndTime) < 0);
        }
    }

    /** This tests focuses on invoking certificate renewal without renewing the ClientsCA.
     * Steps are following. Create own clients CA. From the CA create a Bundle that will be used in kafka cluster which is then deployed.
     * Once everything is set up, kafka reconciliation is paused using annotation. Then new validityDays and renewalDays are set for Clients CA.
     * When annotation is removed, kafka resumes and tries to renew user certificates using the CA with different validityDays and renewalDays.
     * Test also verifies that the CA has not been renewed, only the certificates provided by the CA.
     */
    @ParallelNamespaceTest
    void testReplaceCustomClientsCACertificateValidityToInvokeRenewalProcess() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final int renewalDays = 15;
        final int newRenewalDays = 150;
        final int validityDays = 20;
        final int newValidityDays = 200;

        final SystemTestCertHolder clientsCa = new SystemTestCertHolder(
            "CN=" + testStorage.getTestName() + "ClientsCA",
            KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName()),
            KafkaResources.clientsCaKeySecretName(testStorage.getClusterName()));

        prepareTestCaWithBundleAndKafkaCluster(clientsCa, testStorage, renewalDays, validityDays);

        resourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage).build());
        final Map<String, String> entityPods = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(testStorage.getClusterName()));

        // Check initial clientsCA validity days
        Secret clientsCASecret = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName()));
        X509Certificate cacert = SecretUtils.getCertificateFromSecret(clientsCASecret, "ca.crt");
        final Date initialCertStartTime = cacert.getNotBefore();
        final Date initialCertEndTime = cacert.getNotAfter();

        // Check initial kafkauser validity days
        X509Certificate userCert = SecretUtils.getCertificateFromSecret(kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), testStorage.getUsername()), "user.crt");
        final Date initialKafkaUserCertStartTime = userCert.getNotBefore();
        final Date initialKafkaUserCertEndTime = userCert.getNotAfter();

        // Pause Kafka reconciliation
        LOGGER.info("Pause the reconciliation of the Kafka custom resource ({})", StrimziPodSetResource.getBrokerComponentName(testStorage.getClusterName()));
        KafkaUtils.annotateKafka(testStorage.getClusterName(), testStorage.getNamespaceName(), Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true"));

        LOGGER.info("Change of Kafka validity and renewal days - reconciliation should start");
        final CertificateAuthority newClientsCA = new CertificateAuthority();
        newClientsCA.setRenewalDays(newRenewalDays);
        newClientsCA.setValidityDays(newValidityDays);
        newClientsCA.setGenerateCertificateAuthority(false);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> k.getSpec().setClientsCa(newClientsCA), testStorage.getNamespaceName());

        // Resume Kafka reconciliation
        LOGGER.info("Resume the reconciliation of the Kafka custom resource ({})", StrimziPodSetResource.getBrokerComponentName(testStorage.getClusterName()));
        KafkaUtils.removeAnnotation(testStorage.getClusterName(), testStorage.getNamespaceName(), Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION);

        // Wait for reconciliation and verify certs have been updated
        DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), testStorage.getEoDeploymentName(), 1, entityPods);

        // Read renewed secret/certs again
        clientsCASecret = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName()));
        cacert = SecretUtils.getCertificateFromSecret(clientsCASecret, "ca.crt");
        final Date changedCertStartTime = cacert.getNotBefore();
        final Date changedCertEndTime = cacert.getNotAfter();

        userCert = SecretUtils.getCertificateFromSecret(kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), testStorage.getUsername()), "user.crt");
        final Date changedKafkaUserCertStartTime = userCert.getNotBefore();
        final Date changedKafkaUserCertEndTime = userCert.getNotAfter();

        // Print out certificate dates for debug
        LOGGER.info("Initial ClientsCA cert dates: " + initialCertStartTime + " --> " + initialCertEndTime);
        LOGGER.info("Changed ClientsCA cert dates: " + changedCertStartTime + " --> " + changedCertEndTime);
        LOGGER.info("Initial userCert dates: " + initialKafkaUserCertStartTime + " --> " + initialKafkaUserCertEndTime);
        LOGGER.info("Changed userCert dates: " + changedKafkaUserCertStartTime + " --> " + changedKafkaUserCertEndTime);

        // Verify renewal process
        assertThat("ClientsCA cert should not have changed.", initialCertEndTime.compareTo(changedCertEndTime) == 0);
        assertThat("UserCert start date has been renewed", initialKafkaUserCertStartTime.compareTo(changedKafkaUserCertStartTime) < 0);
        assertThat("UserCert end date has been renewed", initialKafkaUserCertEndTime.compareTo(changedKafkaUserCertEndTime) < 0);
    }

    /**
     * This method simulates user doing manual renewal of Cluster Authority. Process consists of:
     * 1. Pause Kafka reconciliation
     * 2. Retain old ca.crt value in new key in format of YEAR-MONTH-DAY'T'HOUR-MINUTE-SECOND'Z'
     * 3. Change ca.crt and ca.key values in secrets from oldCa to newCa
     * 4. Increase cert annotation value by one
     * 5. Patch the secrets to apply mentioned changes
     * 6. Resume Kafka reconciliation
     * Based on which CA is changed, different rolling update checks should be done
     * @param testStorage is necessary for common variables like namespace name and cluster name
     * @param oldCa represents currently deployed CA which will be replaced - needed for retaining ca.crt value
     * @param newCa stands for a CA which will be used after the renewal is done
     */
    private void manuallyRenewCa(TestStorage testStorage, SystemTestCertHolder oldCa, SystemTestCertHolder newCa) {
        // Pause Kafka reconciliation
        LOGGER.info("Pause the reconciliation of the Kafka custom resource ({})", StrimziPodSetResource.getBrokerComponentName(testStorage.getClusterName()));
        KafkaUtils.annotateKafka(testStorage.getClusterName(), testStorage.getNamespaceName(), Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true"));

        String certSecretName = "";
        String keySecretName = "";

        if (oldCa.getCaCertSecretName().equals(KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()))) {
            certSecretName = KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName());
            keySecretName = KafkaResources.clusterCaKeySecretName(testStorage.getClusterName());
        } else {
            certSecretName = KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName());
            keySecretName = KafkaResources.clientsCaKeySecretName(testStorage.getClusterName());
        }

        // Retrieve current CA cert and key
        Secret caCertificateSecret = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), certSecretName);
        Secret caKeySecret = kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(), keySecretName);

        // Get old certifcate ca.crt key name in format of YEAR-MONTH-DAY'T'HOUR-MINUTE-SECOND'Z'
        final String oldCaCertName = oldCa.retrieveOldCertificateName(caCertificateSecret, "ca.crt");
        caCertificateSecret.getData().put(oldCaCertName, caCertificateSecret.getData().get("ca.crt"));

        try {
            // Put new CA cert into the ca-cert secret
            caCertificateSecret.getData().put("ca.crt", Base64.getEncoder().encodeToString(Files.readAllBytes(Paths.get(newCa.getBundle().getCertPath()))));
            // Put new CA key into the ca secret
            final File strimziKeyPKCS8 = SystemTestCertManager.convertPrivateKeyToPKCS8File(newCa.getSystemTestCa().getPrivateKey());
            caKeySecret.getData().put("ca.key", Base64.getEncoder().encodeToString(Files.readAllBytes(Paths.get(strimziKeyPKCS8.getAbsolutePath()))));
        } catch (IOException | NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException(e);
        }

        // Patch secrets with new values and increase generation counter
        SystemTestCertHolder.increaseCertGenerationCounterInSecret(caCertificateSecret, testStorage, Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION);
        SystemTestCertHolder.increaseCertGenerationCounterInSecret(caKeySecret, testStorage, Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION);

        // Resume Kafka reconciliation
        LOGGER.info("Resume the reconciliation of the Kafka custom resource ({})", StrimziPodSetResource.getBrokerComponentName(testStorage.getClusterName()));
        KafkaUtils.removeAnnotation(testStorage.getClusterName(), testStorage.getNamespaceName(), Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION);
    }

    private void checkCustomCaCorrectness(final SystemTestCertHolder caHolder, final X509Certificate certificate) {
        LOGGER.info("Check ClusterCA and ClientsCA certificates");
        assertThat("Generated ClientsCA or ClusterCA does not have expected Issuer: " + certificate.getIssuerDN(),
            SystemTestCertManager.containsAllDN(certificate.getIssuerX500Principal().getName(), STRIMZI_INTERMEDIATE_CA));
        assertThat("Generated ClientsCA or ClusterCA does not have expected Subject: " + certificate.getSubjectDN(),
            SystemTestCertManager.containsAllDN(certificate.getSubjectX500Principal().getName(), caHolder.getSubjectDn()));
    }

    /**
     * Provides preparation for {@link #testReplacingCustomClientsKeyPairToInvokeRenewalProcess()} and
     * {@link #testReplacingCustomClusterKeyPairToInvokeRenewalProcess()} test cases. This consists of
     * creation of CA with bundles, deployment of Kafka cluster and eventually pausing the reconciliation for specific
     * Kafka cluster to proceed with updating public or private keys.
     *
     * @param certificateAuthority          certificate authority of Clients or Cluster
     * @param testStorage                            auxiliary resources for test case
     */
    private void prepareTestCaWithBundleAndKafkaCluster(final SystemTestCertHolder certificateAuthority,
                                                        final TestStorage testStorage, int renewalDays, int validityDays) {

        // 1. Prepare correspondent Secrets from generated custom CA certificates
        //  a) Cluster or Clients CA
        certificateAuthority.prepareCustomSecretsFromBundles(testStorage.getNamespaceName(), testStorage.getClusterName());

        final X509Certificate certificate = SecretUtils.getCertificateFromSecret(kubeClient(testStorage.getNamespaceName()).getSecret(testStorage.getNamespaceName(),
                certificateAuthority.getCaCertSecretName()), "ca.crt");
        checkCustomCaCorrectness(certificateAuthority, certificate);

        //  b) if Cluster CA is under test - we generate custom Clients CA (it's because in our Kafka configuration we
        //     specify to not generate CA automatically. Thus we need it generate on our own to avoid issue
        //     (f.e., Clients CA should not be generated, but the secrets were not found.)
        if (certificateAuthority.getCaCertSecretName().equals(KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName())) &&
            certificateAuthority.getCaKeySecretName().equals(KafkaResources.clusterCaKeySecretName(testStorage.getClusterName()))) {
            final SystemTestCertHolder clientsCa = new SystemTestCertHolder(
                "CN=" + testStorage.getTestName() + "ClientsCA",
                KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName()),
                KafkaResources.clientsCaKeySecretName(testStorage.getClusterName()));
            clientsCa.prepareCustomSecretsFromBundles(testStorage.getNamespaceName(), testStorage.getClusterName());
        } else {
            // otherwise we generate Cluster CA
            final SystemTestCertHolder clusterCa = new SystemTestCertHolder(
                "CN=" + testStorage.getTestName() + "ClusterCA",
                KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()),
                KafkaResources.clusterCaKeySecretName(testStorage.getClusterName()));
            clusterCa.prepareCustomSecretsFromBundles(testStorage.getNamespaceName(), testStorage.getClusterName());
        }

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        // 2. Create a Kafka cluster without implicit generation of CA and paused reconciliation
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3)
            .editOrNewSpec()
                .withNewClientsCa()
                    .withRenewalDays(renewalDays)
                    .withValidityDays(validityDays)
                    .withGenerateCertificateAuthority(false)
                .endClientsCa()
                .withNewClusterCa()
                    .withRenewalDays(renewalDays)
                    .withValidityDays(validityDays)
                    .withGenerateCertificateAuthority(false)
                .endClusterCa()
            .endSpec()
            .build());
    }

    @BeforeAll
    void setup() {
        this.clusterOperator = this.clusterOperator
                .defaultInstallation()
                .createInstallation()
                .runInstallation();
    }

}
