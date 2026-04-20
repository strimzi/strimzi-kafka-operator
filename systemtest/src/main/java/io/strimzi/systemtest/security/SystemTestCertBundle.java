/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.dsl.base.PatchContext;
import io.fabric8.kubernetes.client.dsl.base.PatchType;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.skodjob.kubetest4j.security.CertAndKey;
import io.skodjob.kubetest4j.security.CertAndKeyFiles;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

import static io.strimzi.systemtest.security.SystemTestCertGenerator.convertPrivateKeyToPKCS8Format;

/**
 * Provides representation of custom chain key-pair i.e., {@code strimziRootCa}, {@code intermediateCa},
 * {@code systemTestClusterCa} and {@code systemTestClusterCa}. Moreover, it allows to generate a new Client or Cluster
 * key-pair used for instance in renewal process.
 */
public class SystemTestCertBundle {

    private static final Logger LOGGER = LogManager.getLogger(SystemTestCertBundle.class);

    private final String caCertSecretName;
    private final String caKeySecretName;
    private final String subjectDn;

    private final CertAndKey strimziRootCa;
    private final CertAndKey intermediateCa;
    private final CertAndKey systemTestCa;
    private final CertAndKeyFiles bundle;

    public SystemTestCertBundle(final String cnName, final String caCertSecretName, final String caKeySecretName) {
        this.strimziRootCa = SystemTestCertGenerator.generateRootCaCertAndKey();
        this.intermediateCa = SystemTestCertGenerator.generateIntermediateCaCertAndKey(this.strimziRootCa);
        this.subjectDn = "C=CZ, L=Prague, O=StrimziTest, " + cnName;

        this.systemTestCa = SystemTestCertGenerator.generateStrimziCaCertAndKey(this.intermediateCa, this.subjectDn);

        // Create PEM bundles (strimzi root CA, intermediate CA, cluster|clients CA cert+key) for ClusterCA and ClientsCA
        this.bundle = SystemTestCertGenerator.exportToPemFiles(this.systemTestCa, this.intermediateCa, this.strimziRootCa);

        this.caCertSecretName = caCertSecretName;
        this.caKeySecretName = caKeySecretName;
    }

    private SystemTestCertBundle(final String namespaceName, final String clusterName) {
        this.strimziRootCa = SystemTestCertGenerator.generateRootCaCertAndKey();
        this.intermediateCa = SystemTestCertGenerator.generateIntermediateCaCertAndKey(this.strimziRootCa);
        this.subjectDn = SystemTestCertGenerator.STRIMZI_END_SUBJECT;
        this.systemTestCa = SystemTestCertGenerator.generateEndEntityCertAndKey(
            this.intermediateCa, SystemTestCertGenerator.retrieveKafkaBrokerSANs(namespaceName, clusterName));
        this.bundle = SystemTestCertGenerator.exportToPemFiles(
            this.systemTestCa, this.intermediateCa, this.strimziRootCa);
        this.caCertSecretName = null;
        this.caKeySecretName = null;
    }

    /**
     * Creates a certificate bundle with a broker end-entity certificate (with broker SANs)
     * as the leaf, instead of a CA certificate. The end-entity certificate is not a CA
     * (i.e., it has no CA basic constraint), so it cannot be used to sign further certificates.
     * Useful for custom broker cert chain tests.
     *
     * @param namespaceName the Kubernetes namespace in which the Kafka cluster is deployed
     * @param clusterName   the name of the Kafka cluster
     * @return a new SystemTestCertBundle with root → intermediate → end-entity (non-CA) hierarchy
     */
    public static SystemTestCertBundle forBrokerEndEntityCertificate(final String namespaceName, final String clusterName) {
        return new SystemTestCertBundle(namespaceName, clusterName);
    }

    /**
     * Prepares custom Cluster and Clients key-pair and creates related Secrets with initialization of annotation
     * {@code Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION} and {@code Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION}.
     *
     * @param namespaceName name of the Namespace, where we create such key-pair and related Secrets
     * @param clusterName name of the Kafka cluster, which is associated with generated key-pair
     */
    public void createCustomSecretsFromBundles(final String namespaceName, final String clusterName) {
        final Map<String, String> additionalSecretLabels = Map.of(
            Labels.STRIMZI_CLUSTER_LABEL, clusterName,
            Labels.STRIMZI_KIND_LABEL, "Kafka");

        try {
            LOGGER.info("Deploying Certificate authority ({}-{}) for Kafka cluster {}/{} and create associated Secrets",
                this.caCertSecretName, this.caKeySecretName, namespaceName, clusterName);
            // 1. Replace the CA certificate generated by the Cluster Operator.
            //      a) Delete the existing secret.
            SecretUtils.deleteSecretWithWait(namespaceName, this.caCertSecretName);
            //      b) Create the new (custom) secret.
            SecretUtils.createCustomCertSecret(namespaceName, clusterName, this.caCertSecretName, this.bundle);

            // 2. Replace the private key generated by the Cluster Operator and 3. Labeling of the secrets is done in creation phase
            //      a) Delete the existing secret.
            SecretUtils.deleteSecretWithWait(namespaceName, this.caKeySecretName);
            final File strimziKeyPKCS8 = convertPrivateKeyToPKCS8Format(this.systemTestCa.getPrivateKey());
            //      b) Create the new (custom) secret.

            Secret secret = SecretUtils.createSecretFromFile(namespaceName, "ca.key", this.caKeySecretName, strimziKeyPKCS8.getAbsolutePath(), additionalSecretLabels);
            KubeResourceManager.get().createResourceWithWait(secret);

            // 4. Annotate the secrets
            SecretUtils.annotateSecret(namespaceName, this.caCertSecretName, Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "0");
            SecretUtils.annotateSecret(namespaceName, this.caKeySecretName, Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "0");
        } catch (NoSuchAlgorithmException | InvalidKeySpecException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String retrieveOldCertificateName(final Secret s, final String dataKey) {
        final X509Certificate caCert = SecretUtils.getCertificateFromSecret(s, dataKey);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(caCert.getNotAfter().toInstant(), ZoneId.systemDefault());

        // old name
        return "ca-" + localDateTime.toString().replaceAll(":", "-") + "." + dataKey.split("\\.")[1];
    }

    public static void patchSecretAndIncreaseGeneration(final Secret secret, final TestStorage testStorage, final String annotationKey) {
        Map<String, String> clusterCaSecretAnnotations = secret.getMetadata().getAnnotations();
        if (clusterCaSecretAnnotations == null) {
            clusterCaSecretAnnotations = new HashMap<>();
        }
        if (clusterCaSecretAnnotations.containsKey(annotationKey)) {
            int generationNumber = Integer.parseInt(clusterCaSecretAnnotations.get(annotationKey));
            clusterCaSecretAnnotations.put(annotationKey, String.valueOf(++generationNumber));
        }
        KubeResourceManager.get().kubeClient().getClient()
            .secrets().inNamespace(testStorage.getNamespaceName()).withName(secret.getMetadata().getName()).patch(PatchContext.of(PatchType.JSON), secret);
    }

    /**
     * Creates a SystemTestCertBundle for the ClusterCA.
     *
     * @param testStorage storage with information about test, which is used to create custom key-pair and related Secrets
     * @return SystemTestCertBundle with prepared custom key-pair and related Secrets
     */
    public static SystemTestCertBundle forClusterCa(final TestStorage testStorage) {
        return new SystemTestCertBundle(
            "CN=" + testStorage.getTestName() + "ClusterCA",
            KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()),
            KafkaResources.clusterCaKeySecretName(testStorage.getClusterName()));
    }

    /**
     * Creates a SystemTestCertBundle for the ClientsCA.
     *
     * @param testStorage storage with information about test, which is used to create custom key-pair and related Secrets
     * @return SystemTestCertBundle with prepared custom key-pair and related Secrets
     */
    public static SystemTestCertBundle forClientsCa(final TestStorage testStorage) {
        return new SystemTestCertBundle(
            "CN=" + testStorage.getTestName() + "ClientsCA",
            KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName()),
            KafkaResources.clientsCaKeySecretName(testStorage.getClusterName()));
    }

    public CertAndKey getStrimziRootCa() {
        return strimziRootCa;
    }
    public CertAndKey getIntermediateCa() {
        return intermediateCa;
    }
    public CertAndKey getSystemTestCa() {
        return systemTestCa;
    }
    public CertAndKeyFiles getBundle() {
        return bundle;
    }
    public String getCaCertSecretName() {
        return caCertSecretName;
    }
    public String getCaKeySecretName() {
        return caKeySecretName;
    }

    public String getSubjectDn() {
        return subjectDn;
    }
}
