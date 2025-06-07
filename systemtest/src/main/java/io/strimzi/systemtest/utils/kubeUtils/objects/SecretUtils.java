/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.objects;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.skodjob.testframe.enums.LogLevel;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.skodjob.testframe.security.CertAndKeyFiles;
import io.skodjob.testframe.security.OpenSsl;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.security.SystemTestCertGenerator;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SecretUtils {

    private static final Logger LOGGER = LogManager.getLogger(SecretUtils.class);
    private static final long READINESS_TIMEOUT = ResourceOperation.getTimeoutForResourceReadiness(TestConstants.SECRET);
    private static final long DELETION_TIMEOUT = ResourceOperation.getTimeoutForResourceDeletion();

    private SecretUtils() { }

    public static void waitForSecretReady(String namespaceName, String secretName, Runnable onTimeout) {
        LOGGER.info("Waiting for Secret: {}/{} to exist", namespaceName, secretName);
        TestUtils.waitFor("Secret: " + secretName + " to exist", TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, READINESS_TIMEOUT,
            () -> KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(namespaceName).withName(secretName).get() != null,
            onTimeout);
        LOGGER.info("Secret: {}/{} created", namespaceName, secretName);
    }

    public static void waitForSecretDeletion(final String namespaceName, String secretName, Runnable onTimeout) {
        LOGGER.info("Waiting for Secret: {}/{} deletion", namespaceName, secretName);
        TestUtils.waitFor("Secret: " + namespaceName + "/" + secretName + " deletion", TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, READINESS_TIMEOUT,
            () -> KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(namespaceName).withName(secretName).get() == null,
            onTimeout);
        LOGGER.info("Secret: {}/{} deleted", namespaceName, secretName);
    }

    public static void waitForSecretDeletion(final String namespaceName, String secretName) {
        waitForSecretDeletion(namespaceName, secretName, () -> { });
    }

    public static void createSecret(String namespaceName, String secretName, String dataKey, String dataValue) {
        LOGGER.info("Creating Secret: {}/{}", namespaceName, secretName);
        KubeResourceManager.get().createResourceWithWait(new SecretBuilder()
            .withApiVersion("v1")
            .withKind("Secret")
            .withNewMetadata()
                .withName(secretName)
                .withNamespace(namespaceName)
            .endMetadata()
            .withType("Opaque")
                .withStringData(Collections.singletonMap(dataKey, dataValue))
            .build());
    }

    public static Secret createSecretFromFile(String namespaceName, String key, String name, String pathToOrigin, Map<String, String> labels) {
        return createSecretFromFile(namespaceName, name, Collections.singletonMap(key, pathToOrigin), labels);
    }

    public static Secret createSecretFromFile(String namespaceName, String name, Map<String, String> certFilesPath, Map<String, String> labels) {
        byte[] encoded;
        try {
            Map<String, String> data = new HashMap<>();
            for (Map.Entry<String, String> entry : certFilesPath.entrySet()) {
                encoded = Files.readAllBytes(Paths.get(entry.getValue()));

                Base64.Encoder encoder = Base64.getEncoder();
                data.put(entry.getKey(), encoder.encodeToString(encoded));
            }

            return new SecretBuilder()
                .withData(data)
                    .withNewMetadata()
                        .withName(name)
                        .withNamespace(namespaceName)
                        .addToLabels(labels)
                    .endMetadata()
                .build();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static SecretBuilder retrieveSecretBuilderFromFile(final String namespaceName, final String name, final Map<String, String> certFilesPath,
                                                              final Map<String, String> labels,
                                                              final String secretType) {
        byte[] encoded;
        final Map<String, String> data = new HashMap<>();

        try {
            for (final Map.Entry<String, String> entry : certFilesPath.entrySet()) {
                encoded = Files.readAllBytes(Paths.get(entry.getValue()));

                final Base64.Encoder encoder = Base64.getEncoder();
                data.put(entry.getKey(), encoder.encodeToString(encoded));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new SecretBuilder()
            .withType(secretType)
            .withData(data)
                .withNewMetadata()
                .withName(name)
                .withNamespace(namespaceName)
                .addToLabels(labels)
            .endMetadata();
    }

    public static void createCustomCertSecret(String namespaceName, String clusterName, String name, CertAndKeyFiles certAndKeyFiles) {
        createCustomCertSecret(namespaceName, clusterName, name, certAndKeyFiles, "ca");
    }

    public static void createCustomCertSecret(String namespaceName, String clusterName, String name, CertAndKeyFiles certAndKeyFiles, String dataPathPrefix) {
        Map<String, String> secretLabels = new HashMap<>();
        secretLabels.put(Labels.STRIMZI_CLUSTER_LABEL, clusterName);
        secretLabels.put(Labels.STRIMZI_KIND_LABEL, "Kafka");

        Map<String, String> certsPaths = new HashMap<>();
        certsPaths.put(dataPathPrefix + ".crt", certAndKeyFiles.getCertPath());
        certsPaths.put(dataPathPrefix + ".key", certAndKeyFiles.getKeyPath());

        Secret secret = createSecretFromFile(namespaceName, name, certsPaths, secretLabels);
        KubeResourceManager.get().createResourceWithWait(secret);
        waitForSecretReady(namespaceName, name, () -> { });
    }

    public static void updateCustomCertSecret(String namespaceName, String clusterName, String name, CertAndKeyFiles certAndKeyFiles) {
        updateCustomCertSecret(namespaceName, clusterName, name, certAndKeyFiles, "ca");
    }

    public static void updateCustomCertSecret(String namespaceName, String clusterName, String name, CertAndKeyFiles certAndKeyFiles, String dataPathPrefix) {
        Map<String, String> secretLabels = new HashMap<>();
        secretLabels.put(Labels.STRIMZI_CLUSTER_LABEL, clusterName);
        secretLabels.put(Labels.STRIMZI_KIND_LABEL, "Kafka");

        Map<String, String> certsPaths = new HashMap<>();
        certsPaths.put(dataPathPrefix + ".crt", certAndKeyFiles.getCertPath());
        certsPaths.put(dataPathPrefix + ".key", certAndKeyFiles.getKeyPath());

        Secret secret = createSecretFromFile(namespaceName, name, certsPaths, secretLabels);
        KubeResourceManager.get().updateResource(secret);
        waitForSecretReady(namespaceName, name, () -> { });
    }

    public static void waitForCertToChange(String namespaceName, String originalCert, String secretName) {
        LOGGER.info("Waiting for Secret: {}/{} certificate to be replaced", namespaceName, secretName);
        TestUtils.waitFor("cert to be replaced", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.TIMEOUT_FOR_CLUSTER_STABLE, () -> {
            Secret secret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(namespaceName).withName(secretName).get();
            if (secret != null && secret.getData() != null && secret.getData().containsKey("ca.crt")) {
                String currentCert = Util.decodeFromBase64((secret.getData().get("ca.crt")));
                boolean changed = !originalCert.equals(currentCert);
                if (changed) {
                    LOGGER.info("Certificate in Secret: {}/{} changed, was: {}, is now: {}", namespaceName, secretName, originalCert, currentCert);
                }
                return changed;
            } else {
                return false;
            }
        });
    }

    public static void deleteSecretWithWait(String namespaceName, String secretName) {
        KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(namespaceName).withName(secretName).delete();

        LOGGER.info("Waiting for Secret: {}/{} to be deleted", namespaceName, secretName);
        TestUtils.waitFor(String.format("Deletion of Secret %s#%s", namespaceName, secretName), TestConstants.GLOBAL_POLL_INTERVAL, DELETION_TIMEOUT,
            () -> KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(namespaceName).withName(secretName).get() == null);

        LOGGER.info("Secret: {}/{} successfully deleted", namespaceName, secretName);
    }

    public static X509Certificate getCertificateFromSecret(Secret secret, String dataKey) {
        String caCert = secret.getData().get(dataKey);
        byte[] decoded = Util.decodeBytesFromBase64(caCert);
        X509Certificate cacert = null;
        try {
            cacert = (X509Certificate)
                    CertificateFactory.getInstance("X.509").generateCertificate(new ByteArrayInputStream(decoded));
        } catch (CertificateException e) {
            e.printStackTrace();
        }
        return cacert;
    }

    public static void waitForUserPasswordChange(String namespaceName, String secretName, String expectedEncodedPassword) {
        LOGGER.info("Waiting for user password will be changed to {} in Secret: {}/{}", expectedEncodedPassword, namespaceName, secretName);
        TestUtils.waitFor(String.format("user password will be changed to: %s in secret: %s(%s)", expectedEncodedPassword, namespaceName, secretName),
            TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(namespaceName).withName(secretName)
                .get().getData().get("password").equals(expectedEncodedPassword));
    }

    public static String annotateSecret(String namespaceName, String secretName, String annotationKey, String annotationValue) {
        LOGGER.info("Annotating Secret: {}/{} with annotation {}={}", namespaceName, secretName, annotationKey, annotationValue);
        return KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName)
                .exec(LogLevel.DEBUG, "annotate", "secret", secretName, annotationKey + "=" + annotationValue)
                .out()
                .trim();
    }

    public static SecretBuilder createCopyOfSecret(String fromNamespace, String toNamespace, String secretName) {
        Secret desiredSecret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(fromNamespace).withName(secretName).get();

        return new SecretBuilder(desiredSecret)
            .withNewMetadata()
                .withName(secretName)
                .withNamespace(toNamespace)
            .endMetadata();
    }

    /**
     * To test tls-external authentication, client needs to have an external certificates provided
     * To simulate externally provided certs, these steps are done:
     * 1. Generate private key and csr (containing at least common name CN) for user
     * 2. Generate a certificate by signing CSR using private key from secret generated for clients by operator
     * 3. Create user secret from private key, generated certificate and certificate from secret created for clients by operator
     * @param namespaceName Namespace name where kafka is deployed
     * @param userName User name of kafkaUser
     * @param clusterName Name of cluster which kafkaUser belongs to
     */
    public static void createExternalTlsUserSecret(String namespaceName, String userName, String clusterName) {
        try {
            File clientPrivateKey = OpenSsl.generatePrivateKey();

            File csr = OpenSsl.generateCertSigningRequest(clientPrivateKey, "/CN=" + userName);
            String caCrt = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(namespaceName).withName(KafkaResources.clientsCaCertificateSecretName(clusterName)).get().getData().get("ca.crt");
            String caKey = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(namespaceName).withName(KafkaResources.clientsCaKeySecretName(clusterName)).get().getData().get("ca.key");

            File clientCert = OpenSsl.generateSignedCert(csr,
                                                         SystemTestCertGenerator.exportCaDataToFile(Util.decodeFromBase64(caCrt, StandardCharsets.UTF_8), "ca", ".crt"),
                                                         SystemTestCertGenerator.exportCaDataToFile(Util.decodeFromBase64(caKey, StandardCharsets.UTF_8), "ca", ".key"));

            Secret secretBuilder = new SecretBuilder()
                .withNewMetadata()
                    .withName(userName)
                    .withNamespace(namespaceName)
                .endMetadata()
                .addToData("ca.crt", caCrt)
                .addToData("user.crt", Base64.getEncoder().encodeToString(Files.readAllBytes(clientCert.toPath())))
                .addToData("user.key", Base64.getEncoder().encodeToString(Files.readAllBytes(clientPrivateKey.toPath())))
                .withType("Opaque")
                .build();

            KubeResourceManager.get().createResourceWithWait(secretBuilder);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
