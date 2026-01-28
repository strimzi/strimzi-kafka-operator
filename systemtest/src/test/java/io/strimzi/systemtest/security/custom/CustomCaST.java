/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.custom;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.dsl.base.PatchContext;
import io.fabric8.kubernetes.client.dsl.base.PatchType;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.common.CertificateAuthority;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.resources.crd.KafkaComponents;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.security.SystemTestCertBundle;
import io.strimzi.systemtest.security.SystemTestCertGenerator;
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

import static io.strimzi.systemtest.TestTags.REGRESSION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag(REGRESSION)
@SuiteDoc(
    description = @Desc("Test suite for verifying custom certificate authority (CA) key pair manipulation, including replacing cluster and clients key pairs to trigger the renewal process."),
    labels = {
        @Label(value = TestDocsLabels.SECURITY)
    }
)
public class CustomCaST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(CustomCaST.class);
    private static final String STRIMZI_INTERMEDIATE_CA = "C=CZ, L=Prague, O=Strimzi, CN=StrimziIntermediateCA";

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("This test verifies manual renewal of a custom cluster CA by replacing the cluster CA key pair and triggering certificate renewal through rolling updates."),
        steps = {
            @Step(value = "Create a custom cluster CA and deploy Kafka cluster with it.", expected = "Kafka cluster is deployed with custom cluster CA."),
            @Step(value = "Generate a new cluster CA key pair.", expected = "New cluster CA is created."),
            @Step(value = "Replace the old cluster CA key pair with the new one, while retaining the old certificate.", expected = "CA secrets are updated with new key pair."),
            @Step(value = "Resume reconciliation and wait for rolling updates to complete.", expected = "All components roll to trust the new CA and use new certificates."),
            @Step(value = "Verify message production works with renewed certificates.", expected = "Producer successfully sends messages."),
            @Step(value = "Remove outdated certificate and trigger manual rolling update using 'strimzi.io/manual-rolling-update' annotation.", expected = "Cluster no longer trusts old certificates.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    void testReplacingCustomClusterKeyPairToInvokeRenewalProcess() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        // Generate root and intermediate certificate authority with cluster CA
        SystemTestCertBundle clusterCa = new SystemTestCertBundle(
            "CN=" + testStorage.getTestName() + "ClusterCA",
            KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()),
            KafkaResources.clusterCaKeySecretName(testStorage.getClusterName()));

        prepareTestCaWithBundleAndKafkaCluster(clusterCa, testStorage, 5, 20);

        // Take snapshots for later comparison
        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        Map<String, String> controllerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());
        Map<String, String> eoPod = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), testStorage.getEoDeploymentName());

        // Create new CA and renew old one with it
        final SystemTestCertBundle newClusterCa = new SystemTestCertBundle(
            "CN=" + testStorage.getTestName() + "ClusterCAv2",
            KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()),
            KafkaResources.clusterCaKeySecretName(testStorage.getClusterName()));

        // Get old certificate ca.crt field name
        Secret clusterCaCertificateSecret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName())).get();
        final String oldCaCertName = clusterCa.retrieveOldCertificateName(clusterCaCertificateSecret, "ca.crt");

        manuallyRenewCa(testStorage, clusterCa, newClusterCa);

        // On the next reconciliation, the Cluster Operator performs a `rolling update`:
        //      a) Kafka controllers
        //      b) Kafka brokers
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
        DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), testStorage.getEoDeploymentName(), 1, eoPod);

        LOGGER.info("All rounds of rolling update have been finished");
        // Try to produce messages
        final KafkaClients kafkaBasicClientJob = ClientUtils.getInstantTlsClients(testStorage);
        KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage).build());
        KubeResourceManager.get().createResourceWithWait(kafkaBasicClientJob.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getProducerName(), testStorage.getMessageCount());

        // Get new certificate secret and assert old value is still present
        clusterCaCertificateSecret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName())).get();
        assertNotNull(clusterCaCertificateSecret.getData().get(oldCaCertName));

        // Remove outdated certificate from the secret configuration to ensure that the cluster no longer trusts them.
        clusterCaCertificateSecret.getData().remove(oldCaCertName);
        KubeResourceManager.get().kubeClient().getClient()
            .secrets().inNamespace(testStorage.getNamespaceName()).withName(clusterCaCertificateSecret.getMetadata().getName()).patch(PatchContext.of(PatchType.JSON), clusterCaCertificateSecret);

        // Start a manual rolling update of your cluster to pick up the changes made to the secret configuration.
        StrimziPodSetUtils.annotateStrimziPodSet(testStorage.getNamespaceName(), testStorage.getControllerComponentName(), Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true"));
        StrimziPodSetUtils.annotateStrimziPodSet(testStorage.getNamespaceName(), testStorage.getBrokerComponentName(), Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true"));
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getControllerSelector(), 3, controllerPods);
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);

    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("This test verifies manual renewal of a custom clients CA by replacing the clients CA key pair and triggering certificate renewal through rolling updates. Only Kafka pods should roll, Entity Operator must not roll."),
        steps = {
            @Step(value = "Create custom clients CA and deploy Kafka cluster with it.", expected = "Kafka cluster is deployed with custom clients CA."),
            @Step(value = "Generate a new clients CA key-pair.", expected = "New clients CA is created."),
            @Step(value = "Replace the old clients CA key pair with the new one while retaining the old certificate.", expected = "CA secrets are updated with new key pair."),
            @Step(value = "Resume reconciliation and wait for Kafka pods to roll.", expected = "Kafka pods roll to trust the new CA and use new certificates."),
            @Step(value = "Verify Entity Operator does not roll.", expected = "Entity Operator pods remain unchanged."),
            @Step(value = "Verify message production works with renewed certificates.", expected = "Producer successfully sends messages.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    void testReplacingCustomClientsKeyPairToInvokeRenewalProcess() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        // Generate root and intermediate certificate authority with clients CA
        final SystemTestCertBundle clientsCa = new SystemTestCertBundle(
            "CN=" + testStorage.getTestName() + "ClientsCA",
            KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName()),
            KafkaResources.clientsCaKeySecretName(testStorage.getClusterName()));

        prepareTestCaWithBundleAndKafkaCluster(clientsCa, testStorage, 5, 20);

        // Take snapshots for later comparison
        final Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        final Map<String, String> controllerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());
        final Map<String, String> eoPod = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), testStorage.getEoDeploymentName());

        LOGGER.info("Generating a new custom 'User certificate authority' with `Root` and `Intermediate` for Strimzi and PEM bundles");
        final SystemTestCertBundle newClientsCa = new SystemTestCertBundle(
            "CN=" + testStorage.getTestName() + "ClientsCAv2",
            KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName()),
            KafkaResources.clientsCaKeySecretName(testStorage.getClusterName()));

        manuallyRenewCa(testStorage, clientsCa, newClientsCa);

        // On the next reconciliation, the Cluster Operator performs a `rolling update` only for the
        // `Kafka pods`. When the rolling update is complete, the Cluster Operator will start a new one to
        // generate new server certificates signed by the new CA key.

        // Kafka has to roll
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getControllerSelector(), 3, brokerPods);
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);

        // EO must not roll
        RollingUpdateUtils.waitForNoRollingUpdate(testStorage.getNamespaceName(), testStorage.getEoSelector(), eoPod);

        // Try to produce messages
        final KafkaClients kafkaBasicClientJob = ClientUtils.getInstantTlsClients(testStorage);
        KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage).build());
        KubeResourceManager.get().createResourceWithWait(kafkaBasicClientJob.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getProducerName(), testStorage.getMessageCount());
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("This test verifies the use of custom cluster and clients CAs. Custom CAs are created and deployed as secrets before Kafka deployment, which forces Kafka to use them instead of generating its own certificate authorities."),
        steps = {
            @Step(value = "Create custom cluster and clients CAs and deploy them as secrets.", expected = "Both CA secrets are created."),
            @Step(value = "Deploy Kafka cluster configured to use custom CAs.", expected = "Kafka cluster is deployed using the custom CAs."),
            @Step(value = "Verify that Kafka broker certificates are signed by the custom cluster CA.", expected = "Broker certificates have the correct issuer."),
            @Step(value = "Create a KafkaUser and verify that its certificate is signed by the custom clients CA.", expected = "User certificate has the correct issuer."),
            @Step(value = "Send and receive messages over TLS.", expected = "Messages are successfully produced and consumed.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    void testCustomClusterCaAndClientsCaCertificates() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        final SystemTestCertBundle clientsCa = new SystemTestCertBundle(
            "CN=" + testStorage.getTestName() + "ClientsCA",
            KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName()),
            KafkaResources.clientsCaKeySecretName(testStorage.getClusterName()));
        final SystemTestCertBundle clusterCa = new SystemTestCertBundle(
            "CN=" + testStorage.getTestName() + "ClusterCA",
            KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()),
            KafkaResources.clusterCaKeySecretName(testStorage.getClusterName()));

        // prepare custom Ca and copy that to the related Secrets
        clientsCa.createCustomSecretsFromBundles(testStorage.getNamespaceName(), testStorage.getClusterName());
        clusterCa.createCustomSecretsFromBundles(testStorage.getNamespaceName(), testStorage.getClusterName());

        final X509Certificate clientsCert = SecretUtils.getCertificateFromSecret(KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName())).get(), "ca.crt");
        final X509Certificate clusterCert = SecretUtils.getCertificateFromSecret(KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName())).get(), "ca.crt");

        checkCustomCaCorrectness(clientsCa, clientsCert);
        checkCustomCaCorrectness(clusterCa, clusterCert);

        LOGGER.info("Deploying Kafka with new certs, Secrets");
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
            .build()
        );

        LOGGER.info("Check Kafka(s) certificates");
        String brokerPodName = KubeResourceManager.get().kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector()).get(0).getMetadata().getName();
        final X509Certificate kafkaCert = SecretUtils.getCertificateFromSecret(KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(brokerPodName).get(), brokerPodName + ".crt");
        assertThat("KafkaCert does not have expected test Issuer: " + kafkaCert.getIssuerDN(),
                SystemTestCertGenerator.containsAllDN(kafkaCert.getIssuerX500Principal().getName(), clusterCa.getSubjectDn()));

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());

        LOGGER.info("Check KafkaUser certificate");
        final KafkaUser user = KafkaUserTemplates.tlsUser(testStorage).build();
        KubeResourceManager.get().createResourceWithWait(user);
        final X509Certificate userCert = SecretUtils.getCertificateFromSecret(KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getUsername()).get(), "user.crt");
        assertThat("Generated ClientsCA does not have expected test Subject: " + userCert.getIssuerDN(),
                SystemTestCertGenerator.containsAllDN(userCert.getIssuerX500Principal().getName(), clientsCa.getSubjectDn()));

        LOGGER.info("Send and receive messages over TLS");
        final KafkaClients kafkaClients = ClientUtils.getInstantTlsClients(testStorage);
        KubeResourceManager.get().createResourceWithWait(kafkaClients.producerTlsStrimzi(testStorage.getClusterName()), kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantClientSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("This test verifies that changing certificate validity and renewal days triggers renewal of cluster certificates without renewing the cluster CA itself."),
        steps = {
            @Step(value = "Create a custom cluster CA and deploy Kafka cluster with it.", expected = "Kafka cluster is deployed with custom cluster CA."),
            @Step(value = "Record initial CA and broker certificate dates.", expected = "Certificate dates are captured."),
            @Step(value = "Pause Kafka reconciliation and update validity and renewal days for cluster CA.", expected = "CA configuration is updated."),
            @Step(value = "Resume reconciliation and wait for components to roll.", expected = "Controllers, brokers, and Entity Operator roll."),
            @Step(value = "Verify CA certificate dates remain unchanged.", expected = "Cluster CA was not renewed."),
            @Step(value = "Verify broker certificates have been renewed with new dates.", expected = "Broker certificates have updated validity dates.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    void testReplaceCustomClusterCACertificateValidityToInvokeRenewalProcess() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final int renewalDays = 15;
        final int newRenewalDays = 150;
        final int validityDays = 20;
        final int newValidityDays = 200;

        SystemTestCertBundle clusterCa = new SystemTestCertBundle(
            "CN=" + testStorage.getTestName() + "ClusterCA",
            KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()),
            KafkaResources.clusterCaKeySecretName(testStorage.getClusterName()));

        prepareTestCaWithBundleAndKafkaCluster(clusterCa, testStorage, renewalDays, validityDays);

        // Make snapshots with original CA
        final Map<String, String> controllerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());
        final Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        final Map<String, String> eoPod = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), testStorage.getEoDeploymentName());

        Secret clusterCASecret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName())).get();
        X509Certificate cacert = SecretUtils.getCertificateFromSecret(clusterCASecret, "ca.crt");
        final Date initialCertStartTime = cacert.getNotBefore();
        final Date initialCertEndTime = cacert.getNotAfter();

        // Check Broker kafka certificate dates
        String brokerPodName = KubeResourceManager.get().kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector()).get(0).getMetadata().getName();
        Secret brokerCertCreationSecret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(brokerPodName).get();
        X509Certificate kafkaBrokerCert = SecretUtils.getCertificateFromSecret(brokerCertCreationSecret, brokerPodName + ".crt");
        final Date initialKafkaBrokerCertStartTime = kafkaBrokerCert.getNotBefore();
        final Date initialKafkaBrokerCertEndTime = kafkaBrokerCert.getNotAfter();

        // Pause Kafka reconciliation
        LOGGER.info("Pause the reconciliation of the Kafka CustomResource ({})", KafkaComponents.getBrokerPodSetName(testStorage.getClusterName()));
        KafkaUtils.annotateKafka(testStorage.getNamespaceName(), testStorage.getClusterName(), Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true"));

        // To test trigger of renewal of CA with short validity dates, both new dates need to be set
        // Otherwise we apply completely new CA using retained old certificate to change manually

        LOGGER.info("Change of Kafka validity and renewal days - reconciliation should start");
        final CertificateAuthority newClusterCA = new CertificateAuthority();
        newClusterCA.setRenewalDays(newRenewalDays);
        newClusterCA.setValidityDays(newValidityDays);
        newClusterCA.setGenerateCertificateAuthority(false);

        KafkaUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), k -> k.getSpec().setClusterCa(newClusterCA));

        // Resume Kafka reconciliation
        LOGGER.info("Resume the reconciliation of the Kafka CustomResource ({})", KafkaComponents.getBrokerPodSetName(testStorage.getClusterName()));
        KafkaUtils.removeAnnotation(testStorage.getNamespaceName(), testStorage.getClusterName(), Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION);

        // On the next reconciliation, the Cluster Operator performs a `rolling update`:
        //   a) controllers
        //   b) brokers
        //   c) and other components to trust the new Cluster CA certificate. (i.e., Entity Operator)
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getControllerSelector(), 3, controllerPods);
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);
        DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), testStorage.getEoDeploymentName(), 1, eoPod);

        // Read renewed secret/certs again
        clusterCASecret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName())).get();
        cacert = SecretUtils.getCertificateFromSecret(clusterCASecret, "ca.crt");
        final Date changedCertStartTime = cacert.getNotBefore();
        final Date changedCertEndTime = cacert.getNotAfter();

        // Check renewed Broker kafka certificate dates
        brokerCertCreationSecret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(brokerPodName).get();
        kafkaBrokerCert = SecretUtils.getCertificateFromSecret(brokerCertCreationSecret, brokerPodName + ".crt");
        final Date changedKafkaBrokerCertStartTime = kafkaBrokerCert.getNotBefore();
        final Date changedKafkaBrokerCertEndTime = kafkaBrokerCert.getNotAfter();

        // Print out certificate dates for debug
        LOGGER.info("Initial ClusterCA cert dates: {} --> {}", initialCertStartTime, initialCertEndTime);
        LOGGER.info("Changed ClusterCA cert dates: {} --> {}", changedCertStartTime, changedCertEndTime);
        LOGGER.info("Kafka Broker cert creation dates: {} --> {}", initialKafkaBrokerCertStartTime, initialKafkaBrokerCertEndTime);
        LOGGER.info("Kafka Broker cert changed dates:  {} --> {}", changedKafkaBrokerCertStartTime, changedKafkaBrokerCertEndTime);

        // Verify renewal result
        assertThat("ClusterCA cert should not have changed start date.", initialCertEndTime.compareTo(changedCertEndTime) == 0);
        assertThat("Broker certificates start dates should have been renewed.", initialKafkaBrokerCertStartTime.compareTo(changedKafkaBrokerCertStartTime) < 0);
        assertThat("Broker certificates end dates should have been renewed.", initialKafkaBrokerCertEndTime.compareTo(changedKafkaBrokerCertEndTime) < 0);
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("This test verifies that changing certificate validity and renewal days triggers renewal of user certificates without renewing the clients CA."),
        steps = {
            @Step(value = "Create a custom clients CA, deploy a Kafka cluster, and create a KafkaUser.", expected = "Kafka cluster and user are deployed with custom clients CA."),
            @Step(value = "Record initial CA and user certificate dates.", expected = "Certificate dates are captured."),
            @Step(value = "Pause Kafka reconciliation and update validity and renewal days for clients CA.", expected = "CA configuration is updated."),
            @Step(value = "Resume reconciliation and wait for Entity Operator to roll.", expected = "Entity Operator rolls to apply new configuration."),
            @Step(value = "Verify CA certificate dates remain unchanged.", expected = "Clients CA was not renewed."),
            @Step(value = "Verify user certificates have been renewed with new dates.", expected = "User certificates have updated validity dates.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    void testReplaceCustomClientsCACertificateValidityToInvokeRenewalProcess() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final int renewalDays = 15;
        final int newRenewalDays = 150;
        final int validityDays = 20;
        final int newValidityDays = 200;

        final SystemTestCertBundle clientsCa = new SystemTestCertBundle(
            "CN=" + testStorage.getTestName() + "ClientsCA",
            KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName()),
            KafkaResources.clientsCaKeySecretName(testStorage.getClusterName()));

        prepareTestCaWithBundleAndKafkaCluster(clientsCa, testStorage, renewalDays, validityDays);

        KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage).build());
        final Map<String, String> entityPods = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(testStorage.getClusterName()));

        // Check initial clientsCA validity days
        Secret clientsCASecret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName())).get();
        X509Certificate cacert = SecretUtils.getCertificateFromSecret(clientsCASecret, "ca.crt");
        final Date initialCertStartTime = cacert.getNotBefore();
        final Date initialCertEndTime = cacert.getNotAfter();

        // Check initial kafkauser validity days
        X509Certificate userCert = SecretUtils.getCertificateFromSecret(
            KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getUsername()).get(),
            "user.crt"
        );
        final Date initialKafkaUserCertStartTime = userCert.getNotBefore();
        final Date initialKafkaUserCertEndTime = userCert.getNotAfter();

        // Pause Kafka reconciliation
        LOGGER.info("Pause the reconciliation of the Kafka CustomResource ({})", KafkaComponents.getBrokerPodSetName(testStorage.getClusterName()));
        KafkaUtils.annotateKafka(testStorage.getNamespaceName(), testStorage.getClusterName(), Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true"));

        LOGGER.info("Change of Kafka validity and renewal days - reconciliation should start");
        final CertificateAuthority newClientsCA = new CertificateAuthority();
        newClientsCA.setRenewalDays(newRenewalDays);
        newClientsCA.setValidityDays(newValidityDays);
        newClientsCA.setGenerateCertificateAuthority(false);

        KafkaUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), k -> k.getSpec().setClientsCa(newClientsCA));

        // Resume Kafka reconciliation
        LOGGER.info("Resume the reconciliation of the Kafka CustomResource ({})", KafkaComponents.getBrokerPodSetName(testStorage.getClusterName()));
        KafkaUtils.removeAnnotation(testStorage.getNamespaceName(), testStorage.getClusterName(), Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION);

        // Wait for reconciliation and verify certs have been updated
        DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), testStorage.getEoDeploymentName(), 1, entityPods);

        // Read renewed secret/certs again
        clientsCASecret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName())
                .withName(KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName())).get();
        cacert = SecretUtils.getCertificateFromSecret(clientsCASecret, "ca.crt");
        final Date changedCertStartTime = cacert.getNotBefore();
        final Date changedCertEndTime = cacert.getNotAfter();

        userCert = SecretUtils.getCertificateFromSecret(
            KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getUsername()).get(),
            "user.crt"
        );
        final Date changedKafkaUserCertStartTime = userCert.getNotBefore();
        final Date changedKafkaUserCertEndTime = userCert.getNotAfter();

        // Print out certificate dates for debug
        LOGGER.info("Initial ClientsCA cert dates: {} --> {}", initialCertStartTime, initialCertEndTime);
        LOGGER.info("Changed ClientsCA cert dates: {} --> {}", changedCertStartTime, changedCertEndTime);
        LOGGER.info("Initial userCert dates: {} --> {}", initialKafkaUserCertStartTime, initialKafkaUserCertEndTime);
        LOGGER.info("Changed userCert dates: {} --> {}", changedKafkaUserCertStartTime, changedKafkaUserCertEndTime);

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
    private void manuallyRenewCa(TestStorage testStorage, SystemTestCertBundle oldCa, SystemTestCertBundle newCa) {
        // Pause Kafka reconciliation
        LOGGER.info("Pause the reconciliation of the Kafka CustomResource ({})", KafkaComponents.getBrokerPodSetName(testStorage.getClusterName()));
        KafkaUtils.annotateKafka(testStorage.getNamespaceName(), testStorage.getClusterName(), Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true"));

        String certSecretName;
        String keySecretName;

        if (oldCa.getCaCertSecretName().equals(KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()))) {
            certSecretName = KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName());
            keySecretName = KafkaResources.clusterCaKeySecretName(testStorage.getClusterName());
        } else {
            certSecretName = KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName());
            keySecretName = KafkaResources.clientsCaKeySecretName(testStorage.getClusterName());
        }

        // Retrieve current CA cert and key
        Secret caCertificateSecret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(certSecretName).get();
        Secret caKeySecret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(keySecretName).get();

        // Get old certificate ca.crt key name in format of YEAR-MONTH-DAY'T'HOUR-MINUTE-SECOND'Z'
        final String oldCaCertName = oldCa.retrieveOldCertificateName(caCertificateSecret, "ca.crt");
        caCertificateSecret.getData().put(oldCaCertName, caCertificateSecret.getData().get("ca.crt"));

        try {
            // Put new CA cert into the ca-cert secret
            caCertificateSecret.getData().put("ca.crt", Base64.getEncoder().encodeToString(Files.readAllBytes(Paths.get(newCa.getBundle().getCertPath()))));
            // Put new CA key into the ca secret
            final File strimziKeyPKCS8 = SystemTestCertGenerator.convertPrivateKeyToPKCS8Format(newCa.getSystemTestCa().getPrivateKey());
            caKeySecret.getData().put("ca.key", Base64.getEncoder().encodeToString(Files.readAllBytes(Paths.get(strimziKeyPKCS8.getAbsolutePath()))));
        } catch (IOException | NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException(e);
        }

        // Patch secrets with new values and increase generation counter
        SystemTestCertBundle.patchSecretAndIncreaseGeneration(caCertificateSecret, testStorage, Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION);
        SystemTestCertBundle.patchSecretAndIncreaseGeneration(caKeySecret, testStorage, Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION);

        // Resume Kafka reconciliation
        LOGGER.info("Resume the reconciliation of the Kafka CustomResource ({})", KafkaComponents.getBrokerPodSetName(testStorage.getClusterName()));
        KafkaUtils.removeAnnotation(testStorage.getNamespaceName(), testStorage.getClusterName(), Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION);
    }

    private void checkCustomCaCorrectness(final SystemTestCertBundle caHolder, final X509Certificate certificate) {
        LOGGER.info("Check ClusterCA and ClientsCA certificates");
        assertThat("Generated ClientsCA or ClusterCA does not have expected Issuer: " + certificate.getIssuerDN(),
            SystemTestCertGenerator.containsAllDN(certificate.getIssuerX500Principal().getName(), STRIMZI_INTERMEDIATE_CA));
        assertThat("Generated ClientsCA or ClusterCA does not have expected Subject: " + certificate.getSubjectDN(),
            SystemTestCertGenerator.containsAllDN(certificate.getSubjectX500Principal().getName(), caHolder.getSubjectDn()));
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
    private void prepareTestCaWithBundleAndKafkaCluster(final SystemTestCertBundle certificateAuthority,
                                                        final TestStorage testStorage, int renewalDays, int validityDays) {

        // 1. Prepare correspondent Secrets from generated custom CA certificates
        //  a) Cluster or Clients CA
        certificateAuthority.createCustomSecretsFromBundles(testStorage.getNamespaceName(), testStorage.getClusterName());

        final X509Certificate certificate = SecretUtils.getCertificateFromSecret(
            KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(certificateAuthority.getCaCertSecretName()).get(), "ca.crt"
        );
        checkCustomCaCorrectness(certificateAuthority, certificate);

        //  b) if Cluster CA is under test - we generate custom Clients CA (it's because in our Kafka configuration we
        //     specify to not generate CA automatically. Thus we need it generate on our own to avoid issue
        //     (f.e., Clients CA should not be generated, but the secrets were not found.)
        if (certificateAuthority.getCaCertSecretName().equals(KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName())) &&
            certificateAuthority.getCaKeySecretName().equals(KafkaResources.clusterCaKeySecretName(testStorage.getClusterName()))) {
            final SystemTestCertBundle clientsCa = new SystemTestCertBundle(
                "CN=" + testStorage.getTestName() + "ClientsCA",
                KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName()),
                KafkaResources.clientsCaKeySecretName(testStorage.getClusterName()));
            clientsCa.createCustomSecretsFromBundles(testStorage.getNamespaceName(), testStorage.getClusterName());
        } else {
            // otherwise we generate Cluster CA
            final SystemTestCertBundle clusterCa = new SystemTestCertBundle(
                "CN=" + testStorage.getTestName() + "ClusterCA",
                KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()),
                KafkaResources.clusterCaKeySecretName(testStorage.getClusterName()));
            clusterCa.createCustomSecretsFromBundles(testStorage.getNamespaceName(), testStorage.getClusterName());
        }

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        // 2. Create a Kafka cluster without implicit generation of CA and paused reconciliation
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
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
        SetupClusterOperator
            .getInstance()
            .withDefaultConfiguration()
            .install();
    }

}
