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
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.annotations.ParallelSuite;
import io.strimzi.systemtest.enums.CustomResourceStatus;
import io.strimzi.systemtest.kafkaclients.clients.InternalKafkaClient;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.security.SystemTestCertHolder;
import io.strimzi.systemtest.security.SystemTestCertManager;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Provides test cases to verify custom key-pair (public key + private key) manipulation. For instance:
 *  1. Replacing `cluster` key-pair (f.e., ca.crt and ca.key) to invoke renewal process
 *  2. Replacing `clients` key-pair (f.e., user.crt and user.key) to invoke renewal process
 *
 * {@link https://strimzi.io/docs/operators/in-development/configuring.html#installing-your-own-ca-certificates-str}
 * {@link https://strimzi.io/docs/operators/in-development/configuring.html#proc-replacing-your-own-private-keys-str}
 */
@ParallelSuite
public class CustomCaST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(CustomCaST.class);
    private static final String STRIMZI_INTERMEDIATE_CA = "C=CZ, L=Prague, O=Strimzi, CN=StrimziIntermediateCA";

    private final String namespace = testSuiteNamespaceManager.getMapOfAdditionalNamespaces().get(CustomCaST.class.getSimpleName()).stream().findFirst().get();

    @ParallelNamespaceTest
    void testReplacingCustomClusterKeyPairToInvokeRenewalProcess(ExtensionContext extensionContext) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        final TestStorage testStorage = new TestStorage(extensionContext);

        this.replaceCustomCaToInvokeRenewalProcess(extensionContext,
            // 0. Generate root and intermediate certificate authority with clients CA
            new SystemTestCertHolder(extensionContext,
                "CN=" + extensionContext.getRequiredTestClass().getSimpleName() + "ClusterCA",
                KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()),
                KafkaResources.clusterCaKeySecretName(testStorage.getClusterName())),
            testStorage,
            // ------- public key part
            (ts, clusterCa) -> {
                // 5. Update the Secret for the CA certificate.
                //  a) Edit the existing secret to add the new CA certificate and update the certificate generation annotation value.
                //  b) Rename the current CA certificate to retain it
                final Secret clusterCaCertificateSecret = kubeClient(ts.getNamespaceName()).getSecret(ts.getNamespaceName(), KafkaResources.clusterCaCertificateSecretName(ts.getClusterName()));
                final String oldCaCertName = clusterCa.retrieveOldCertificateName(clusterCaCertificateSecret, "ca.crt");

                // store the old cert
                clusterCaCertificateSecret.getData().put(oldCaCertName, clusterCaCertificateSecret.getData().get("ca.crt"));

                //  c) Encode your new CA certificate into base64.
                LOGGER.info("Generating a new custom 'Cluster certificate authority' with `Root` and `Intermediate` for Strimzi and PEM bundles.");
                clusterCa = new SystemTestCertHolder(extensionContext,
                    "CN=" + extensionContext.getRequiredTestClass().getSimpleName() + "ClusterCA",
                    KafkaResources.clusterCaCertificateSecretName(ts.getClusterName()),
                    KafkaResources.clusterCaKeySecretName(ts.getClusterName()));

                //  d) Update the CA certificate.
                try {
                    clusterCaCertificateSecret.getData().put("ca.crt", Base64.getEncoder().encodeToString(Files.readAllBytes(Paths.get(clusterCa.getBundle().getCertPath()))));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                //  e) Increase the value of the CA certificate generation annotation.
                //  f) Save the secret with the new CA certificate and certificate generation annotation value.
                SystemTestCertHolder.increaseCertGenerationCounterInSecret(clusterCaCertificateSecret, ts, Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION);
                return clusterCa;
            },
            // ------- private key part
            (ts, clusterCa) -> {
                // 6. Update the Secret for the CA key used to sign your new CA certificate.
                //  a) Edit the existing secret to add the new CA key and update the key generation annotation value.
                final Secret clusterCaKeySecret = kubeClient(ts.getNamespaceName()).getSecret(ts.getNamespaceName(), KafkaResources.clusterCaKeySecretName(ts.getClusterName()));

                //  b) Encode the CA key into base64.
                //  c) Update the CA key.
                File strimziKeyPKCS8 = null;
                try {
                    strimziKeyPKCS8 = SystemTestCertManager.convertPrivateKeyToPKCS8File(clusterCa.getSystemTestCa().getPrivateKey());
                    clusterCaKeySecret.getData().put("ca.key", Base64.getEncoder().encodeToString(Files.readAllBytes(Paths.get(strimziKeyPKCS8.getAbsolutePath()))));
                } catch (NoSuchAlgorithmException | InvalidKeySpecException | IOException e) {
                    throw new RuntimeException(e);
                }

                // d) Increase the value of the CA key generation annotation.
                // 7. Save the secret with the new CA key and key generation annotation value.
                SystemTestCertHolder.increaseCertGenerationCounterInSecret(clusterCaKeySecret, ts, Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION);
                return (Void) null;
            },
            ts -> {
                // 8. save the current state of the Kafka, ZooKeeper and EntityOperator pods
                final Map<String, String> kafkaPods = PodUtils.podSnapshot(ts.getNamespaceName(), ts.getKafkaSelector());
                final Map<String, String> zkPods = PodUtils.podSnapshot(ts.getNamespaceName(), ts.getZookeeperSelector());
                final Map<String, String> eoPod = DeploymentUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(ts.getClusterName()));

                // 9. Resume reconciliation from the pause.
                LOGGER.info("Resume the reconciliation of the Kafka custom resource ({}).", KafkaResources.kafkaStatefulSetName(ts.getClusterName()));
                KafkaResource.replaceKafkaResourceInSpecificNamespace(ts.getClusterName(), kafka -> {
                    kafka.getMetadata().getAnnotations().remove(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION);
                }, ts.getNamespaceName());

                // 10. On the next reconciliation, the Cluster Operator performs a `rolling update`:
                //      a) ZooKeeper
                //      b) Kafka
                //      c) and other components to trust the new CA certificate. (i.e., EntityOperator)
                //  When the rolling update is complete, the Cluster Operator
                //  will start a new one to generate new server certificates signed by the new CA key.
                RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(ts.getZookeeperSelector(), 1, zkPods);
                RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(ts.getKafkaSelector(), 1, kafkaPods);
                DeploymentUtils.waitTillDepHasRolled(KafkaResources.entityOperatorDeploymentName(ts.getClusterName()), 1, eoPod);
                return (Void) null;
            });
    }

    @ParallelNamespaceTest
    void testReplacingCustomClientsKeyPairToInvokeRenewalProcess(ExtensionContext extensionContext) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        final TestStorage testStorage = new TestStorage(extensionContext);

        this.replaceCustomCaToInvokeRenewalProcess(extensionContext,
            // 0. Generate root and intermediate certificate authority with clients CA
            new SystemTestCertHolder(extensionContext, "CN=" + extensionContext.getRequiredTestClass().getSimpleName() + "ClientsCA",
                KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName()),
                KafkaResources.clientsCaKeySecretName(testStorage.getClusterName())),
            testStorage,
            // ------- public key part
            (ts, clientsCa) -> {
                // 5. Update the Secret for the CA certificate.
                //  a) Edit the existing secret to add the new CA certificate and update the certificate generation annotation value.
                //  b) Rename the current CA certificate to retain it
                final Secret clientsCaCertificateSecret = kubeClient(ts.getNamespaceName()).getSecret(ts.getNamespaceName(), KafkaResources.clientsCaCertificateSecretName(ts.getClusterName()));
                final String oldCaCertName = clientsCa.retrieveOldCertificateName(clientsCaCertificateSecret, "ca.crt");

                // store the old cert
                clientsCaCertificateSecret.getData().put(oldCaCertName, clientsCaCertificateSecret.getData().get("ca.crt"));

                //  c) Encode your new CA certificate into base64.
                LOGGER.info("Generating a new custom 'User certificate authority' with `Root` and `Intermediate` for Strimzi and PEM bundles.");
                clientsCa = new SystemTestCertHolder(extensionContext,
                    "CN=" + extensionContext.getRequiredTestClass().getSimpleName() + "ClientsCA",
                    KafkaResources.clientsCaCertificateSecretName(ts.getClusterName()),
                    KafkaResources.clientsCaKeySecretName(ts.getClusterName()));

                //  d) Update the CA certificate.
                try {
                    clientsCaCertificateSecret.getData().put("ca.crt", Base64.getEncoder().encodeToString(Files.readAllBytes(Paths.get(clientsCa.getBundle().getCertPath()))));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                //  e) Increase the value of the CA certificate generation annotation.
                //  f) Save the secret with the new CA certificate and certificate generation annotation value.
                SystemTestCertHolder.increaseCertGenerationCounterInSecret(clientsCaCertificateSecret, ts, Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION);
                return clientsCa;
            },
            // ------- private key part
            (ts, clientsCa) -> {
                // 6. Update the Secret for the CA key used to sign your new CA certificate.
                //  a) Edit the existing secret to add the new CA key and update the key generation annotation value.
                final Secret clientsCaKeySecret = kubeClient(ts.getNamespaceName()).getSecret(ts.getNamespaceName(), KafkaResources.clientsCaKeySecretName(ts.getClusterName()));

                //  b) Encode the CA key into base64.
                //  c) Update the CA key.
                File strimziKeyPKCS8 = null;
                try {
                    strimziKeyPKCS8 = SystemTestCertManager.convertPrivateKeyToPKCS8File(clientsCa.getSystemTestCa().getPrivateKey());
                    clientsCaKeySecret.getData().put("ca.key", Base64.getEncoder().encodeToString(Files.readAllBytes(Paths.get(strimziKeyPKCS8.getAbsolutePath()))));

                } catch (NoSuchAlgorithmException | InvalidKeySpecException | IOException e) {
                    throw new RuntimeException(e);
                }
                // d) Increase the value of the CA key generation annotation.
                // 7. Save the secret with the new CA key and key generation annotation value.
                SystemTestCertHolder.increaseCertGenerationCounterInSecret(clientsCaKeySecret, ts, Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION);
                return (Void) null;
            },
            ts -> {
                // 8. save the current state of the Kafka
                final Map<String, String> kafkaPods = PodUtils.podSnapshot(ts.getNamespaceName(), ts.getKafkaSelector());

                // 9. Resume reconciliation from the pause.
                LOGGER.info("Resume the reconciliation of the Kafka custom resource ({}).", KafkaResources.kafkaStatefulSetName(ts.getClusterName()));
                KafkaResource.replaceKafkaResourceInSpecificNamespace(ts.getClusterName(), kafka -> {
                    kafka.getMetadata().getAnnotations().remove(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION);
                }, ts.getNamespaceName());

                // 10. On the next reconciliation, the Cluster Operator performs a `rolling update` only for the
                // Kafka cluster. When the rolling update is complete, the Cluster Operator will start a new one to
                // generate new server certificates signed by the new CA key.
                RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(ts.getKafkaSelector(), 1, kafkaPods);
                return (Void) null;
            });
    }

    /**
     * Helper method providing setup of the test cases for replacing key-pair in cluster or clients. Using BiFunction
     * for parametrizing what can be different in such test cases. For public key part is invoked {@code publicKeyPart}
     * BiFunction and for private key {@code privateKeyPart}.
     *
     * @param extensionContext          context for test case
     * @param certificateAuthority      certificate authority of Clients or Cluster
     * @param ts                        auxiliary resources for test case
     * @param publicKeyPart             BiFunction for the public key part
     * @param privateKeyPart            BiFunction for the private key part
     * @param rollingComponents         Components, which should be rolled (f.e., when we use Clients CA only Kafka components is rolled)
     * @throws NoSuchAlgorithmException if specified algorithm for truststore is not supported
     * @throws IOException              if an input or output file could not be read/written.
     * @throws InvalidKeySpecException  if any problems with reading/writing the truststore
     */
    void replaceCustomCaToInvokeRenewalProcess(final ExtensionContext extensionContext,
                                               SystemTestCertHolder certificateAuthority,
                                               final TestStorage ts,
                                               final BiFunction<TestStorage, SystemTestCertHolder, SystemTestCertHolder> publicKeyPart,
                                               final BiFunction<TestStorage, SystemTestCertHolder, Void> privateKeyPart,
                                               final Function<TestStorage, Void> rollingComponents) throws NoSuchAlgorithmException, IOException, InvalidKeySpecException {
        // 1. Prepare correspondent Secrets from generated custom CA certificates
        //  a) Cluster or Clients CA
        certificateAuthority.prepareCustomSecretsFromBundles(ts.getNamespaceName(), ts.getClusterName());

        //  b) if Cluster CA is under test - we generate custom Clients CA (it's because in our Kafka configuration we
        //     specify to not generate CA automatically. Thus we need it generate on our own to avoid issue
        //     (f.e., Clients CA should not be generated, but the secrets were not found.)
        if (certificateAuthority.getCaCertSecretName().equals(KafkaResources.clusterCaCertificateSecretName(ts.getClusterName())) &&
            certificateAuthority.getCaKeySecretName().equals(KafkaResources.clusterCaKeySecretName(ts.getClusterName()))) {
            final SystemTestCertHolder clientsCa = new SystemTestCertHolder(extensionContext,
                "CN=" + extensionContext.getRequiredTestClass().getSimpleName() + "ClientsCA",
                KafkaResources.clientsCaCertificateSecretName(ts.getClusterName()),
                KafkaResources.clientsCaKeySecretName(ts.getClusterName()));
            clientsCa.prepareCustomSecretsFromBundles(ts.getNamespaceName(), ts.getClusterName());
        } else {
            // otherwise we generate Cluster CA
            final SystemTestCertHolder clusterCa = new SystemTestCertHolder(extensionContext,
                "CN=" + extensionContext.getRequiredTestClass().getSimpleName() + "ClusterCA",
                KafkaResources.clusterCaCertificateSecretName(ts.getClusterName()),
                KafkaResources.clusterCaKeySecretName(ts.getClusterName()));
            clusterCa.prepareCustomSecretsFromBundles(ts.getNamespaceName(), ts.getClusterName());
        }

        // 2. Create a Kafka cluster without implicit generation of CA
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(ts.getClusterName(), 1)
            .editOrNewSpec()
                .withNewClientsCa()
                    .withRenewalDays(5)
                    .withValidityDays(20)
                    .withGenerateCertificateAuthority(false)
                .endClientsCa()
            // TODO: If I un-comment this and run testReplacingCustomClusterKeyPairToInvokeRenewalProcess test it will fail
            //  because only ZooKeeper pod will trigger RollingUpdate, Kafka pod has some errors:
            //  -> https://gist.github.com/see-quick/d32c569572ae396597ce88394dc46fed
//
//               .withNewClusterCa()
//                    .withRenewalDays(5)
//                    .withValidityDays(20)
//                    .withGenerateCertificateAuthority(false)
//                .endClusterCa()
            .endSpec()
            .build());

        // 3. Pause the reconciliation of the Kafka custom resource
        LOGGER.info("Pause the reconciliation of the Kafka custom resource ({}).", KafkaResources.kafkaStatefulSetName(ts.getClusterName()));
        KafkaResource.replaceKafkaResourceInSpecificNamespace(ts.getClusterName(), kafka -> {
            Map<String, String> kafkaAnnotations = kafka.getMetadata().getAnnotations();
            if (kafkaAnnotations == null) {
                kafkaAnnotations = new HashMap<>();
            }
            // adding pause annotation
            kafkaAnnotations.put(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true");
            kafka.getMetadata().setAnnotations(kafkaAnnotations);
        }, ts.getNamespaceName());

        // 4. Check that the status conditions of the custom resource show a change to ReconciliationPaused
        KafkaUtils.waitForKafkaStatus(ts.getNamespaceName(), ts.getClusterName(), CustomResourceStatus.ReconciliationPaused);

        // 5. Update the Secret for the CA certificate. (public key part)
        certificateAuthority = publicKeyPart.apply(ts, certificateAuthority);

        // 6. Update the Secret for the CA key used to sign your new CA certificate. (private key part)
        privateKeyPart.apply(ts, certificateAuthority);

        rollingComponents.apply(ts);

        // 11. Try to produce messages
        final KafkaClients kafkaBasicClientJob = new KafkaClientsBuilder()
            .withProducerName(ts.getProducerName())
            .withConsumerName(ts.getClusterName())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(ts.getClusterName()))
            .withTopicName(ts.getTopicName())
            .withMessageCount(MESSAGE_COUNT)
            .withDelayMs(10)
            .build();

        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(ts.getNamespaceName(), ts.getClusterName(), ts.getUserName()).build());
        resourceManager.createResource(extensionContext, kafkaBasicClientJob.producerTlsStrimzi(ts.getClusterName(), ts.getUserName()));

        ClientUtils.waitForClientSuccess(ts.getProducerName(), ts.getNamespaceName(), MESSAGE_COUNT);
    }

    @ParallelNamespaceTest
    void testCustomClusterCaAndClientsCaCertificates(ExtensionContext extensionContext) {
        final TestStorage ts = new TestStorage(extensionContext);
        final String testSuite = extensionContext.getRequiredTestClass().getSimpleName();

        final SystemTestCertHolder clientsCa = new SystemTestCertHolder(extensionContext,
            "CN=" + testSuite + "ClientsCA",
            KafkaResources.clientsCaCertificateSecretName(ts.getClusterName()),
            KafkaResources.clientsCaKeySecretName(ts.getClusterName()));
        final SystemTestCertHolder clusterCa = new SystemTestCertHolder(extensionContext,
            "CN=" + testSuite + "ClusterCA",
            KafkaResources.clusterCaCertificateSecretName(ts.getClusterName()),
            KafkaResources.clusterCaKeySecretName(ts.getClusterName()));

        // prepare custom Ca and copy that to the related Secrets
        clientsCa.prepareCustomSecretsFromBundles(ts.getNamespaceName(), ts.getClusterName());
        clusterCa.prepareCustomSecretsFromBundles(ts.getNamespaceName(), ts.getClusterName());

        final X509Certificate clientsCert = SecretUtils.getCertificateFromSecret(kubeClient(ts.getNamespaceName()).getSecret(ts.getNamespaceName(),
            KafkaResources.clientsCaCertificateSecretName(ts.getClusterName())), "ca.crt");
        final X509Certificate clusterCert = SecretUtils.getCertificateFromSecret(kubeClient(ts.getNamespaceName()).getSecret(ts.getNamespaceName(),
            KafkaResources.clusterCaCertificateSecretName(ts.getClusterName())), "ca.crt");

        checkCustomCaCorrectness(ts.getNamespaceName(), ts.getClusterName(), clientsCa, clientsCert);
        checkCustomCaCorrectness(ts.getNamespaceName(), ts.getClusterName(), clusterCa, clusterCert);

        LOGGER.info("Deploy kafka with new certs/secrets.");
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(ts.getClusterName(), 3, 3)
            .editSpec()
                .withNewClusterCa()
                    .withGenerateCertificateAuthority(false)
                .endClusterCa()
                .withNewClientsCa()
                    .withGenerateCertificateAuthority(false)
                .endClientsCa()
            .endSpec()
            .build());

        LOGGER.info("Check Kafka(s) and Zookeeper(s) certificates.");
        final X509Certificate kafkaCert = SecretUtils.getCertificateFromSecret(kubeClient(ts.getNamespaceName()).getSecret(ts.getNamespaceName(),
            ts.getClusterName() + "-kafka-brokers"), ts.getClusterName() + "-kafka-0.crt");
        assertThat("KafkaCert does not have expected test Issuer: " + kafkaCert.getIssuerDN(),
                SystemTestCertManager.containsAllDN(kafkaCert.getIssuerX500Principal().getName(), clusterCa.getSubjectDn()));

        X509Certificate zookeeperCert = SecretUtils.getCertificateFromSecret(kubeClient(ts.getNamespaceName()).getSecret(ts.getNamespaceName(),
            ts.getClusterName() + "-zookeeper-nodes"), ts.getClusterName() + "-zookeeper-0.crt");
        assertThat("ZookeeperCert does not have expected test Subject: " + zookeeperCert.getIssuerDN(),
                SystemTestCertManager.containsAllDN(zookeeperCert.getIssuerX500Principal().getName(), clusterCa.getSubjectDn()));

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(ts.getClusterName(), ts.getTopicName()).build());

        LOGGER.info("Check KafkaUser certificate.");
        final KafkaUser user = KafkaUserTemplates.tlsUser(ts.getClusterName(), ts.getUserName()).build();
        resourceManager.createResource(extensionContext, user);
        final X509Certificate userCert = SecretUtils.getCertificateFromSecret(kubeClient(ts.getNamespaceName()).getSecret(ts.getNamespaceName(),
            ts.getUserName()), "user.crt");
        assertThat("Generated ClientsCA does not have expected test Subject: " + userCert.getIssuerDN(),
                SystemTestCertManager.containsAllDN(userCert.getIssuerX500Principal().getName(), clientsCa.getSubjectDn()));

        LOGGER.info("Send and receive messages over TLS.");
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true,
            ts.getClusterName() + "-" + Constants.KAFKA_CLIENTS, user).build());
        final String kafkaClientsPodName = kubeClient(ts.getNamespaceName()).listPodsByPrefixInName(ts.getNamespaceName(),
            ts.getClusterName() + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        final InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(ts.getTopicName())
            .withNamespaceName(ts.getNamespaceName())
            .withClusterName(ts.getClusterName())
            .withKafkaUsername(ts.getUserName())
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        LOGGER.info("Check for certificates used within kafka pod internal clients (producer/consumer)");
        final List<VolumeMount> volumeMounts = kubeClient(ts.getNamespaceName()).listPodsByPrefixInName(ts.getNamespaceName(),
            ts.getClusterName() + "-" + Constants.KAFKA_CLIENTS).get(0).getSpec().getContainers().get(0).getVolumeMounts();
        for (VolumeMount vm : volumeMounts) {
            if (vm.getMountPath().contains("user-secret-" + internalKafkaClient.getKafkaUsername())) {
                assertThat("UserCert Issuer DN in clients pod is incorrect!", checkMountVolumeSecret(ts.getNamespaceName(), kafkaClientsPodName,
                        vm, "issuer", STRIMZI_INTERMEDIATE_CA));
                assertThat("UserCert Subject DN in clients pod is incorrect!", checkMountVolumeSecret(ts.getNamespaceName(), kafkaClientsPodName,
                        vm, "subject", clientsCa.getSubjectDn()));

            } else if (vm.getMountPath().contains("cluster-ca-" + internalKafkaClient.getKafkaUsername())) {
                assertThat("ClusterCA Issuer DN in clients pod is incorrect!", checkMountVolumeSecret(ts.getNamespaceName(), kafkaClientsPodName,
                        vm, "issuer", STRIMZI_INTERMEDIATE_CA));
                assertThat("ClusterCA Subject DN in clients pod is incorrect!", checkMountVolumeSecret(ts.getNamespaceName(), kafkaClientsPodName,
                        vm, "subject", clusterCa.getSubjectDn()));
            }
        }

        LOGGER.info("Checking produced and consumed messages via TLS to pod:{}", kafkaClientsPodName);
        internalKafkaClient.checkProducedAndConsumedMessages(
                internalKafkaClient.sendMessagesTls(),
                internalKafkaClient.receiveMessagesTls()
        );
    }

    private final boolean checkMountVolumeSecret(final String namespaceName, final String podName, final VolumeMount volumeMount,
                                                 final String principalDNType, final String expectedPrincipal) {
        final String dn = cmdKubeClient(namespaceName).execInPod(podName, "/bin/bash", "-c",
            "openssl x509 -in " + volumeMount.getMountPath() + "/ca.crt -noout -nameopt RFC2253 -" + principalDNType).out().strip();
        final String certOutIssuer = dn.substring(principalDNType.length() + 1).replace("/", ",");
        return SystemTestCertManager.containsAllDN(certOutIssuer, expectedPrincipal);
    }

    private final void checkCustomCaCorrectness(final String namespaceName, final String clusterName,
                                                final SystemTestCertHolder caHolder, final X509Certificate certificate) {
        LOGGER.info("Check ClusterCA and ClientsCA certificates.");
        assertThat("Generated ClientsCA or ClusterCA does not have expected Issuer: " + certificate.getIssuerDN(),
            SystemTestCertManager.containsAllDN(certificate.getIssuerX500Principal().getName(), STRIMZI_INTERMEDIATE_CA));
        assertThat("Generated ClientsCA or ClusterCA does not have expected Subject: " + certificate.getSubjectDN(),
            SystemTestCertManager.containsAllDN(certificate.getSubjectX500Principal().getName(), caHolder.getSubjectDn()));
    }

    @ParallelNamespaceTest
    void testCustomClusterCACertificateRenew(ExtensionContext extensionContext) {
        TestStorage ts = new TestStorage(extensionContext);
        final String testSuite = extensionContext.getRequiredTestClass().getSimpleName();

        final SystemTestCertHolder clusterCa = new SystemTestCertHolder(extensionContext,
            "CN=" + testSuite + "ClusterCA",
            KafkaResources.clusterCaCertificateSecretName(ts.getClusterName()),
            KafkaResources.clusterCaKeySecretName(ts.getClusterName()));

        // prepare custom Ca and copy that to the related Secrets
        clusterCa.prepareCustomSecretsFromBundles(ts.getNamespaceName(), ts.getClusterName());

        final X509Certificate clusterCert = SecretUtils.getCertificateFromSecret(kubeClient(ts.getNamespaceName()).getSecret(ts.getNamespaceName(),
            KafkaResources.clusterCaCertificateSecretName(ts.getClusterName())), "ca.crt");

        checkCustomCaCorrectness(ts.getNamespaceName(), ts.getClusterName(), clusterCa, clusterCert);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(ts.getClusterName(), 3)
            .editOrNewSpec()
                // Note: Clients Ca is generated automatically
                .withNewClusterCa()
                    .withRenewalDays(15)
                    .withValidityDays(20)
                    .withGenerateCertificateAuthority(false)
                .endClusterCa()
            .endSpec()
            .build());

        final Map<String, String> kafkaPods = PodUtils.podSnapshot(ts.getNamespaceName(), ts.getKafkaSelector());

        Secret clusterCASecret = kubeClient(ts.getNamespaceName()).getSecret(ts.getNamespaceName(), KafkaResources.clusterCaCertificateSecretName(ts.getClusterName()));
        X509Certificate cacert = SecretUtils.getCertificateFromSecret(clusterCASecret, "ca.crt");
        final Date initialCertStartTime = cacert.getNotBefore();
        final Date initialCertEndTime = cacert.getNotAfter();

        // Check Broker kafka certificate dates
        Secret brokerCertCreationSecret = kubeClient(ts.getNamespaceName()).getSecret(ts.getNamespaceName(), ts.getClusterName() + "-kafka-brokers");
        X509Certificate kafkaBrokerCert = SecretUtils.getCertificateFromSecret(brokerCertCreationSecret, ts.getClusterName() + "-kafka-0.crt");
        final Date initialKafkaBrokerCertStartTime = kafkaBrokerCert.getNotBefore();
        final Date initialKafkaBrokerCertEndTime = kafkaBrokerCert.getNotAfter();

        // Check Zookeeper certificate dates
        Secret zkCertCreationSecret = kubeClient(ts.getNamespaceName()).getSecret(ts.getNamespaceName(), ts.getClusterName() + "-zookeeper-nodes");
        X509Certificate zkBrokerCert = SecretUtils.getCertificateFromSecret(zkCertCreationSecret, ts.getClusterName() + "-zookeeper-0.crt");
        final Date initialZkCertStartTime = zkBrokerCert.getNotBefore();
        final Date initialZkCertEndTime = zkBrokerCert.getNotAfter();

        LOGGER.info("Change of kafka validity and renewal days - reconciliation should start.");
        final CertificateAuthority newClusterCA = new CertificateAuthority();
        newClusterCA.setRenewalDays(150);
        newClusterCA.setValidityDays(200);
        newClusterCA.setGenerateCertificateAuthority(false);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(ts.getClusterName(), k -> k.getSpec().setClusterCa(newClusterCA), ts.getNamespaceName());

        // Wait for reconciliation and verify certs have been updated
        RollingUpdateUtils.waitTillComponentHasRolled(ts.getNamespaceName(), ts.getKafkaSelector(), 3, kafkaPods);

        // Read renewed secret/certs again
        clusterCASecret = kubeClient(ts.getNamespaceName()).getSecret(ts.getNamespaceName(), KafkaResources.clusterCaCertificateSecretName(ts.getClusterName()));
        cacert = SecretUtils.getCertificateFromSecret(clusterCASecret, "ca.crt");
        final Date changedCertStartTime = cacert.getNotBefore();
        final Date changedCertEndTime = cacert.getNotAfter();

        // Check renewed Broker kafka certificate dates
        brokerCertCreationSecret = kubeClient(ts.getNamespaceName()).getSecret(ts.getNamespaceName(), ts.getClusterName() + "-kafka-brokers");
        kafkaBrokerCert = SecretUtils.getCertificateFromSecret(brokerCertCreationSecret, ts.getClusterName() + "-kafka-0.crt");
        final Date changedKafkaBrokerCertStartTime = kafkaBrokerCert.getNotBefore();
        final Date changedKafkaBrokerCertEndTime = kafkaBrokerCert.getNotAfter();

        // Check renewed Zookeeper certificate dates
        zkCertCreationSecret = kubeClient(ts.getNamespaceName()).getSecret(ts.getNamespaceName(), ts.getClusterName() + "-zookeeper-nodes");
        zkBrokerCert = SecretUtils.getCertificateFromSecret(zkCertCreationSecret, ts.getClusterName() + "-zookeeper-0.crt");
        final Date changedZkCertStartTime = zkBrokerCert.getNotBefore();
        final Date changedZkCertEndTime = zkBrokerCert.getNotAfter();

        LOGGER.info("Initial ClusterCA cert dates: " + initialCertStartTime + " --> " + initialCertEndTime);
        LOGGER.info("Changed ClusterCA cert dates: " + changedCertStartTime + " --> " + changedCertEndTime);
        LOGGER.info("KafkaBroker cert creation dates: " + initialKafkaBrokerCertStartTime + " --> " + initialKafkaBrokerCertEndTime);
        LOGGER.info("KafkaBroker cert changed dates:  " + changedKafkaBrokerCertStartTime + " --> " + changedKafkaBrokerCertEndTime);
        LOGGER.info("Zookeeper cert creation dates: " + initialZkCertStartTime + " --> " + initialZkCertEndTime);
        LOGGER.info("Zookeeper cert changed dates:  " + changedZkCertStartTime + " --> " + changedZkCertEndTime);

        assertThat("ClusterCA cert should not have changed.",
            initialCertEndTime.compareTo(changedCertEndTime) == 0);
        assertThat("Broker certificates start dates have not been renewed.",
            initialKafkaBrokerCertStartTime.compareTo(changedKafkaBrokerCertStartTime) < 0);
        assertThat("Broker certificates end dates have not been renewed.",
            initialKafkaBrokerCertEndTime.compareTo(changedKafkaBrokerCertEndTime) < 0);
        assertThat("Zookeeper certificates start dates have not been renewed.",
            initialZkCertStartTime.compareTo(changedZkCertStartTime) < 0);
        assertThat("Zookeeper certificates end dates have not been renewed.",
            initialZkCertEndTime.compareTo(changedZkCertEndTime) < 0);
    }

    @ParallelNamespaceTest
    void testClientsCaCertificateRenew(ExtensionContext extensionContext) {
        final TestStorage ts = new TestStorage(extensionContext);
        final String testSuite = extensionContext.getRequiredTestClass().getSimpleName();

        final SystemTestCertHolder clientsCa = new SystemTestCertHolder(extensionContext,
            "CN=" + testSuite + "ClientsCA",
            KafkaResources.clientsCaCertificateSecretName(ts.getClusterName()),
            KafkaResources.clientsCaKeySecretName(ts.getClusterName()));

        // prepare custom Ca and copy that to the related Secrets
        clientsCa.prepareCustomSecretsFromBundles(ts.getNamespaceName(), ts.getClusterName());

        final X509Certificate clientsCert = SecretUtils.getCertificateFromSecret(kubeClient(ts.getNamespaceName()).getSecret(ts.getNamespaceName(),
            KafkaResources.clientsCaCertificateSecretName(ts.getClusterName())), "ca.crt");

        checkCustomCaCorrectness(ts.getNamespaceName(), ts.getClusterName(), clientsCa, clientsCert);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(ts.getClusterName(), 3)
            .editOrNewSpec()
                // Note: Cluster Ca is generated automatically
                .withNewClientsCa()
                    .withRenewalDays(15)
                    .withValidityDays(20)
                    .withGenerateCertificateAuthority(false)
                .endClientsCa()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(ts.getClusterName(), ts.getUserName()).build());
        final Map<String, String> entityPods = DeploymentUtils.depSnapshot(ts.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(ts.getClusterName()));

        // Check initial clientsCA validity days
        Secret clientsCASecret = kubeClient(ts.getNamespaceName()).getSecret(ts.getNamespaceName(), KafkaResources.clientsCaCertificateSecretName(ts.getClusterName()));
        X509Certificate cacert = SecretUtils.getCertificateFromSecret(clientsCASecret, "ca.crt");
        final Date initialCertStartTime = cacert.getNotBefore();
        final Date initialCertEndTime = cacert.getNotAfter();

        // Check initial kafkauser validity days
        X509Certificate userCert = SecretUtils.getCertificateFromSecret(kubeClient(ts.getNamespaceName()).getSecret(ts.getNamespaceName(), ts.getUserName()), "user.crt");
        final Date initialKafkaUserCertStartTime = userCert.getNotBefore();
        final Date initialKafkaUserCertEndTime = userCert.getNotAfter();

        LOGGER.info("Change of kafka validity and renewal days - reconciliation should start.");
        final CertificateAuthority newClientsCA = new CertificateAuthority();
        newClientsCA.setRenewalDays(150);
        newClientsCA.setValidityDays(200);
        newClientsCA.setGenerateCertificateAuthority(false);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(ts.getClusterName(), k -> k.getSpec().setClientsCa(newClientsCA), ts.getNamespaceName());

        // Wait for reconciliation and verify certs have been updated
        DeploymentUtils.waitTillDepHasRolled(ts.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(ts.getClusterName()), 1, entityPods);

        // Read renewed secret/certs again
        clientsCASecret = kubeClient(ts.getNamespaceName()).getSecret(ts.getNamespaceName(), KafkaResources.clientsCaCertificateSecretName(ts.getClusterName()));
        cacert = SecretUtils.getCertificateFromSecret(clientsCASecret, "ca.crt");
        final Date changedCertStartTime = cacert.getNotBefore();
        final Date changedCertEndTime = cacert.getNotAfter();

        userCert = SecretUtils.getCertificateFromSecret(kubeClient(ts.getNamespaceName()).getSecret(ts.getNamespaceName(), ts.getUserName()), "user.crt");
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
