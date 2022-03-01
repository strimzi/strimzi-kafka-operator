/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.custom;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.annotations.ParallelSuite;
import io.strimzi.systemtest.enums.CustomResourceStatus;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.security.SystemTestCertHolder;
import io.strimzi.systemtest.security.SystemTestCertManager;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

/**
 * Provides test cases to verify custom key-pair (public key + private key) manipulation. For instance:
 *  1. Replacing `cluster` key-pair (f.e., ca.crt and ca.key) to invoke renewal process
 *  2. Replacing `clients` key-pair (f.e., user.crt and user.key) to invoke renewal process
 *
 * {@link https://strimzi.io/docs/operators/in-development/configuring.html#installing-your-own-ca-certificates-str}
 * {@link https://strimzi.io/docs/operators/in-development/configuring.html#proc-replacing-your-own-private-keys-str}
 */
@ParallelSuite
public class CustomKeyPairST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(CustomKeyPairST.class);

    private final String namespace = testSuiteNamespaceManager.getMapOfAdditionalNamespaces().get(CustomKeyPairST.class.getSimpleName()).stream().findFirst().get();

    @ParallelNamespaceTest
    void testReplacingCustomClusterKeyPairToInvokeRenewalProcess(ExtensionContext extensionContext) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        helperMethod(extensionContext,
            // ------- public key part
            (ts, stCertHolder) -> {
                // 5. Update the Secret for the CA certificate.
                //  a) Edit the existing secret to add the new CA certificate and update the certificate generation annotation value.
                //  b) Rename the current CA certificate to retain it
                final Secret clusterCaCertificateSecret = kubeClient(ts.getNamespaceName()).getSecret(ts.getNamespaceName(), KafkaResources.clusterCaCertificateSecretName(ts.getClusterName()));
                final String oldCaCertName = stCertHolder.retrieveOldCertificateName(clusterCaCertificateSecret, "ca.crt");

                // store the old cert
                clusterCaCertificateSecret.getData().put(oldCaCertName, clusterCaCertificateSecret.getData().get("ca.crt"));

                //  c) Encode your new CA certificate into base64. (implicit?)
                LOGGER.info("Generating a new custom 'Cluster certificate authority' for Strimzi and PEM bundles.");
                stCertHolder
                    .generateNewClusterCa(extensionContext)
                    .generateNewClusterBundle(extensionContext);

                //  d) Update the CA certificate.
                try {
                    clusterCaCertificateSecret.getData().put("ca.crt", Base64.getEncoder().encodeToString(Files.readAllBytes(Paths.get(stCertHolder.getClusterBundle().getCertPath()))));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                //  e) Increase the value of the CA certificate generation annotation.
                //  f) Save the secret with the new CA certificate and certificate generation annotation value.
                SystemTestCertHolder.increaseCertGenerationInSecret(clusterCaCertificateSecret, ts, Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION);
                return stCertHolder;
            },
            // ------- private key part
            (ts, stCertHolder) -> {
                // 6. Update the Secret for the CA key used to sign your new CA certificate.
                //  a) Edit the existing secret to add the new CA key and update the key generation annotation value.
                final Secret clusterCaKeySecret = kubeClient(ts.getNamespaceName()).getSecret(ts.getNamespaceName(), KafkaResources.clusterCaKeySecretName(ts.getClusterName()));

                //  b) Encode the CA key into base64.
                //  c) Update the CA key.
                File strimziKeyPKCS8 = null;
                try {
                    strimziKeyPKCS8 = SystemTestCertManager.convertPrivateKeyToPKCS8File(stCertHolder.getSystemTestClusterCa().getPrivateKey());
                    clusterCaKeySecret.getData().put("ca.key", Base64.getEncoder().encodeToString(Files.readAllBytes(Paths.get(strimziKeyPKCS8.getAbsolutePath()))));
                } catch (NoSuchAlgorithmException | InvalidKeySpecException | IOException e) {
                    throw new RuntimeException(e);
                }

                // d) Increase the value of the CA key generation annotation.
                // 7. Save the secret with the new CA key and key generation annotation value.
                SystemTestCertHolder.increaseCertGenerationInSecret(clusterCaKeySecret, ts, Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION);
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
        helperMethod(extensionContext,
            // ------- public key part
            (ts, stCertHolder) -> {
                // 5. Update the Secret for the CA certificate.
                //  a) Edit the existing secret to add the new CA certificate and update the certificate generation annotation value.
                //  b) Rename the current CA certificate to retain it
                final Secret clientsCaCertificateSecret = kubeClient(ts.getNamespaceName()).getSecret(ts.getNamespaceName(), KafkaResources.clientsCaCertificateSecretName(ts.getClusterName()));
                final String oldCaCertName = stCertHolder.retrieveOldCertificateName(clientsCaCertificateSecret, "ca.crt");

                // store the old cert
                clientsCaCertificateSecret.getData().put(oldCaCertName, clientsCaCertificateSecret.getData().get("ca.crt"));

                //  c) Encode your new CA certificate into base64. (implicit?)
                LOGGER.info("Generating a new custom 'User certificate authority' for Strimzi and PEM bundles.");
                stCertHolder
                    .generateNewClientsCa(extensionContext)
                    .generateNewClientsBundle(extensionContext);

                //  d) Update the CA certificate.
                try {
                    clientsCaCertificateSecret.getData().put("ca.crt", Base64.getEncoder().encodeToString(Files.readAllBytes(Paths.get(stCertHolder.getClientsBundle().getCertPath()))));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                //  e) Increase the value of the CA certificate generation annotation.
                //  f) Save the secret with the new CA certificate and certificate generation annotation value.
                SystemTestCertHolder.increaseCertGenerationInSecret(clientsCaCertificateSecret, ts, Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION);
                return stCertHolder;
            },
            // ------- private key part
            (ts, stCertHolder) -> {
                // 6. Update the Secret for the CA key used to sign your new CA certificate.
                //  a) Edit the existing secret to add the new CA key and update the key generation annotation value.
                final Secret clientsCaKeySecret = kubeClient(ts.getNamespaceName()).getSecret(ts.getNamespaceName(), KafkaResources.clientsCaKeySecretName(ts.getClusterName()));

                //  b) Encode the CA key into base64.
                //  c) Update the CA key.
                File strimziKeyPKCS8 = null;
                try {
                    strimziKeyPKCS8 = SystemTestCertManager.convertPrivateKeyToPKCS8File(stCertHolder.getSystemTestClientsCa().getPrivateKey());
                    clientsCaKeySecret.getData().put("ca.key", Base64.getEncoder().encodeToString(Files.readAllBytes(Paths.get(strimziKeyPKCS8.getAbsolutePath()))));

                } catch (NoSuchAlgorithmException | InvalidKeySpecException | IOException e) {
                    throw new RuntimeException(e);
                }
                // d) Increase the value of the CA key generation annotation.
                // 7. Save the secret with the new CA key and key generation annotation value.
                SystemTestCertHolder.increaseCertGenerationInSecret(clientsCaKeySecret, ts, Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION);
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
     * @param publicKeyPart             BiFunction for the public key part
     * @param privateKeyPart            BiFunction for the private key part
     * @param rollingComponents         Components, which should be rolled (f.e., when we use Clients CA only Kafka components is rolled)
     * @throws NoSuchAlgorithmException if specified algorithm for truststore is not supported
     * @throws IOException              if an input or output file could not be read/written.
     * @throws InvalidKeySpecException  if any problems with reading/writing the truststore
     */
    void helperMethod(final ExtensionContext extensionContext, final BiFunction<TestStorage, SystemTestCertHolder, SystemTestCertHolder> publicKeyPart,
                      final BiFunction<TestStorage, SystemTestCertHolder, Void> privateKeyPart,
                      final Function<TestStorage, Void> rollingComponents) throws NoSuchAlgorithmException, IOException, InvalidKeySpecException {
        final TestStorage ts = new TestStorage(extensionContext);

        // 0. Generate root and intermediate certificate authority
        SystemTestCertHolder stCertHolder = new SystemTestCertHolder(extensionContext);

        // 1. Prepare correspondent Secrets from generated custom CA certificates
        stCertHolder.prepareSecretsFromBundles(ts.getNamespaceName(), ts.getClusterName());

        // 2. Create a Kafka cluster without implicit generation of CA
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(ts.getClusterName(), 1)
            .editOrNewSpec()
                .withNewClientsCa()
                    .withRenewalDays(5)
                    .withValidityDays(20)
                    .withGenerateCertificateAuthority(false)
                .endClientsCa()
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
        stCertHolder = publicKeyPart.apply(ts, stCertHolder);

        // 6. Update the Secret for the CA key used to sign your new CA certificate. (private key part)
        privateKeyPart.apply(ts, stCertHolder);

        // 8. save the current state of the Kafka, ZooKeeper and EntityOperator pods
        // 9. Resume reconciliation from the pause.
        // 10. On the next reconciliation, the Cluster Operator performs a `rolling update`:
        //      a) ZooKeeper
        //      b) Kafka
        //      c) and other components to trust the new CA certificate. (i.e., EntityOperator)
        //  When the rolling update is complete, the Cluster Operator
        //  will start a new one to generate new server certificates signed by the new CA key.
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

        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(ts.getNamespaceName(), ts.getClusterName(), ts.getKafkaClientsName()).build());
        resourceManager.createResource(extensionContext, kafkaBasicClientJob.producerTlsStrimzi(ts.getClusterName(), ts.getKafkaClientsName()));

        ClientUtils.waitForClientSuccess(ts.getProducerName(), ts.getNamespaceName(), MESSAGE_COUNT);
    }
}
