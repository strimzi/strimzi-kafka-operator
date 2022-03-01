/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import static io.strimzi.systemtest.security.SystemTestCertManager.convertPrivateKeyToPKCS8File;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

/**
 * Provides representation of custom chain key-pair (i.e., {@code strimziRootCa}, {@code intermediateCa},
 * {@code systemTestClusterCa} and {@code systemTestClusterCa}. Moreover, it allow to generate a new Client or Cluster
 * key-pair used for instance in renewal process.
 */
public class SystemTestCertHolder {

    private static final Logger LOGGER = LogManager.getLogger(SystemTestCertHolder.class);

    private SystemTestCertAndKey strimziRootCa;
    private SystemTestCertAndKey intermediateCa;
    private SystemTestCertAndKey systemTestClusterCa;
    private SystemTestCertAndKey systemTestClientsCa;
    private CertAndKeyFiles clusterBundle;
    private CertAndKeyFiles clientsBundle;

    public SystemTestCertHolder(final ExtensionContext extensionContext) {
        this.strimziRootCa = SystemTestCertManager.generateRootCaCertAndKey();
        this.intermediateCa = SystemTestCertManager.generateIntermediateCaCertAndKey(this.strimziRootCa);

        final String testSuiteName = extensionContext.getRequiredTestClass().getSimpleName();
        final String clusterCa = "C=CZ, L=Prague, O=StrimziTest, CN=" + testSuiteName + "ClusterCA";
        final String clientsCa = "C=CZ, L=Prague, O=StrimziTest, CN=" + testSuiteName + "ClientsCA";

        this.systemTestClusterCa = SystemTestCertManager.generateStrimziCaCertAndKey(this.intermediateCa, clusterCa);
        this.systemTestClientsCa = SystemTestCertManager.generateStrimziCaCertAndKey(this.intermediateCa, clientsCa);

        // Create PEM bundles (strimzi root CA, intermediate CA, cluster|clients CA cert+key) for ClusterCA and ClientsCA
        this.clusterBundle = SystemTestCertManager.exportToPemFiles(this.systemTestClusterCa, this.intermediateCa, this.strimziRootCa);
        this.clientsBundle = SystemTestCertManager.exportToPemFiles(this.systemTestClientsCa, this.intermediateCa, this.strimziRootCa);
    }

    public SystemTestCertHolder generateNewClusterCa(ExtensionContext extensionContext) {
        final String testSuiteName = extensionContext.getRequiredTestClass().getSimpleName();
        final String clusterCa = "C=CZ, L=Prague, O=StrimziTest, CN=" + testSuiteName + "ClusterCA";

        this.systemTestClusterCa = SystemTestCertManager.generateStrimziCaCertAndKey(this.intermediateCa, clusterCa);

        return this;
    }

    public SystemTestCertHolder generateNewClusterBundle(ExtensionContext extensionContext) {
        this.clusterBundle = SystemTestCertManager.exportToPemFiles(this.systemTestClusterCa, this.intermediateCa, this.strimziRootCa);

        return this;
    }

    public SystemTestCertHolder generateNewClientsCa(ExtensionContext extensionContext) {
        final String testSuiteName = extensionContext.getRequiredTestClass().getSimpleName();
        final String clientsCa = "C=CZ, L=Prague, O=StrimziTest, CN=" + testSuiteName + "ClientsCA";

        this.systemTestClientsCa = SystemTestCertManager.generateStrimziCaCertAndKey(this.intermediateCa, clientsCa);

        return this;
    }

    public SystemTestCertHolder generateNewClientsBundle(ExtensionContext extensionContext) {
        this.clientsBundle = SystemTestCertManager.exportToPemFiles(this.systemTestClientsCa, this.intermediateCa, this.strimziRootCa);

        return this;
    }

    /**
     * Prepares custom Cluster and Clients key-pair and creates related Secrets with initialization of annotation
     * {@code Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION} and {@code Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION}.
     *
     * @param namespaceName name of the Namespace, where we creates such key-pair and related Secrets
     * @param clusterName name of the Kafka cluster, which is associated with generated key-pair
     */
    public void prepareSecretsFromBundles(final String namespaceName, final String clusterName) {
        Map<String, String> additionalSecretLabels = Map.of(
            Labels.STRIMZI_CLUSTER_LABEL, clusterName,
            Labels.STRIMZI_KIND_LABEL, "Kafka");

        try {
            // ----------------- Cluster certificates -------------------
            LOGGER.info("Deploy all certificates and keys as secrets.");
            // 1. Replace the CA certificate generated by the Cluster Operator.
            //      a) Delete the existing secret.
            SecretUtils.deleteSecretWithWait(KafkaResources.clusterCaCertificateSecretName(clusterName), namespaceName);
            //      b) Create the new (custom) secret.
            SecretUtils.createCustomSecret(KafkaResources.clusterCaCertificateSecretName(clusterName), clusterName, namespaceName, this.clusterBundle);

            // 2. Replace the private key generated by the Cluster Operator and 3. Labeling of the secrets is done in creation phase
            //      a) Delete the existing secret.
            SecretUtils.deleteSecretWithWait(KafkaResources.clusterCaKeySecretName(clusterName), namespaceName);
            File strimziKeyPKCS8 = convertPrivateKeyToPKCS8File(this.systemTestClusterCa.getPrivateKey());
            //      b) Create the new (custom) secret.
            SecretUtils.createSecretFromFile(strimziKeyPKCS8.getAbsolutePath(), "ca.key", KafkaResources.clusterCaKeySecretName(clusterName), namespaceName, additionalSecretLabels);

            // 4. Annotate the secrets
            SecretUtils.annotateSecret(namespaceName, KafkaResources.clusterCaCertificateSecretName(clusterName), Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "0");
            SecretUtils.annotateSecret(namespaceName, KafkaResources.clusterCaKeySecretName(clusterName), Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "0");

            // ----------------- Clients certificates -------------------
            // 1. Replace the CA certificate generated by the Cluster Operator.
            //      a) Delete the existing secret.
            SecretUtils.deleteSecretWithWait(KafkaResources.clientsCaCertificateSecretName(clusterName), namespaceName);
            //      b) Create the new (custom) secret.
            SecretUtils.createCustomSecret(KafkaResources.clientsCaCertificateSecretName(clusterName), clusterName, namespaceName, this.clientsBundle);

            // 2. Replace the private key generated by the Cluster Operator and 3. Labeling of the secrets is done in creation phase
            //      a) Delete the existing secret.
            SecretUtils.deleteSecretWithWait(KafkaResources.clientsCaKeySecretName(clusterName), namespaceName);
            File clientsKeyPKCS8 = convertPrivateKeyToPKCS8File(this.systemTestClientsCa.getPrivateKey());
            //      b) Create the new (custom) secret.
            SecretUtils.createSecretFromFile(clientsKeyPKCS8.getAbsolutePath(), "ca.key", KafkaResources.clientsCaKeySecretName(clusterName), namespaceName, additionalSecretLabels);

            // 4. Annotate the secrets
            SecretUtils.annotateSecret(namespaceName, KafkaResources.clientsCaCertificateSecretName(clusterName), Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "0");
            SecretUtils.annotateSecret(namespaceName, KafkaResources.clientsCaKeySecretName(clusterName), Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "0");

        } catch (NoSuchAlgorithmException | InvalidKeySpecException | IOException e) {
            e.printStackTrace();
        }
    }

    public String retrieveOldCertificateName(final Secret s, final String dataKey) {
        final X509Certificate caCert = SecretUtils.getCertificateFromSecret(s, dataKey);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(caCert.getNotAfter().toInstant(), ZoneId.systemDefault());
        final String localDateString = localDateTime.format(DateTimeFormatter.ISO_DATE);

        // old name
        return "ca-" + localDateTime.toString().replaceAll(":", "-") + "." + dataKey.split("\\.")[1];
    }

    public static void increaseCertGenerationInSecret(final Secret secret, final TestStorage ts, final String annotationKey) {
        Map<String, String> clusterCaSecretAnnotations = secret.getMetadata().getAnnotations();
        if (clusterCaSecretAnnotations == null) {
            clusterCaSecretAnnotations = new HashMap<>();
        }
        if (clusterCaSecretAnnotations.containsKey(annotationKey)) {
            int generationNumber = Integer.parseInt(clusterCaSecretAnnotations.get(annotationKey));
            clusterCaSecretAnnotations.put(annotationKey, String.valueOf(++generationNumber));
        }
        kubeClient(ts.getNamespaceName()).patchSecret(ts.getNamespaceName(), secret.getMetadata().getName(), secret);
    }

    public SystemTestCertAndKey getStrimziRootCa() {
        return strimziRootCa;
    }
    public SystemTestCertAndKey getIntermediateCa() {
        return intermediateCa;
    }
    public SystemTestCertAndKey getSystemTestClusterCa() {
        return systemTestClusterCa;
    }
    public SystemTestCertAndKey getSystemTestClientsCa() {
        return systemTestClientsCa;
    }
    public CertAndKeyFiles getClusterBundle() {
        return clusterBundle;
    }
    public CertAndKeyFiles getClientsBundle() {
        return clientsBundle;
    }
}
