/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.objects;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class SecretUtils {

    private static final Logger LOGGER = LogManager.getLogger(SecretUtils.class);
    private static final long READINESS_TIMEOUT = ResourceOperation.getTimeoutForResourceReadiness(Constants.SECRET);
    private static final long DELETION_TIMEOUT = ResourceOperation.getTimeoutForResourceDeletion();

    private SecretUtils() { }

    public static void waitForSecretReady(String secretName) {
        waitForSecretReady(secretName, () -> { });
    }

    public static void waitForSecretReady(String secretName, Runnable onTimeout) {
        LOGGER.info("Waiting for Secret {}", secretName);
        TestUtils.waitFor("Expected secret " + secretName + " exists", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, READINESS_TIMEOUT,
            () -> kubeClient().getSecret(secretName) != null,
            onTimeout);
        LOGGER.info("Secret {} created", secretName);
    }

    public static void createSecret(String secretName, String dataKey, String dataValue) {
        LOGGER.info("Creating secret {}", secretName);
        kubeClient().createSecret(new SecretBuilder()
                .withNewApiVersion("v1")
                .withNewKind("Secret")
                .withNewMetadata()
                    .withName(secretName)
                .endMetadata()
                .withNewType("Opaque")
                    .withData(Collections.singletonMap(dataKey, dataValue))
                .build());
    }

    public static void createSecretFromFile(String pathToOrigin, String key, String name, String namespace) {
        createSecretFromFile(Collections.singletonMap(key, pathToOrigin), name, namespace, null);
    }

    public static void createSecretFromFile(Map<String, String> certFilesPath, String name, String namespace) {
        createSecretFromFile(certFilesPath, name, namespace, null);
    }

    public static void createSecretFromFile(Map<String, String> certFilesPath, String name, String namespace, Map<String, String> labels) {
        byte[] encoded;
        try {
            Map<String, String> data = new HashMap<>();
            for (Map.Entry<String, String> entry : certFilesPath.entrySet()) {
                encoded = Files.readAllBytes(Paths.get(entry.getValue()));

                Base64.Encoder encoder = Base64.getEncoder();
                data.put(entry.getKey(), encoder.encodeToString(encoded));
            }

            Secret secret = new SecretBuilder()
                .withData(data)
                    .withNewMetadata()
                        .withName(name)
                        .withNamespace(namespace)
                        .addToLabels(labels)
                    .endMetadata()
                .build();
            kubeClient().namespace(namespace).createSecret(secret);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void waitForClusterSecretsDeletion(String clusterName) {
        LOGGER.info("Waiting for Secret {} deletion", clusterName);
        TestUtils.waitFor("Secret " + clusterName + " deletion", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, DELETION_TIMEOUT,
            () -> {
                List<Secret> secretList = kubeClient().listSecrets(Labels.STRIMZI_CLUSTER_LABEL, clusterName);
                if (secretList.isEmpty()) {
                    return true;
                } else {
                    for (Secret secret : secretList) {
                        LOGGER.warn("Secret {} is not deleted yet! Triggering force delete by cmd client!", secret.getMetadata().getName());
                        cmdKubeClient().deleteByName("secret", secret.getMetadata().getName());
                    }
                    return false;
                }
            });
        LOGGER.info("Secret {} deleted", clusterName);
    }

    public static void createCustomSecret(String name, String clusterName, String namespace, String certPath, String keyPath) {
        Map<String, String> secretLabels = new HashMap<>();
        secretLabels.put(Labels.STRIMZI_CLUSTER_LABEL, clusterName);
        secretLabels.put(Labels.STRIMZI_KIND_LABEL, "Kafka");

        Map<String, String> certsPaths = new HashMap<>();
        certsPaths.put("ca.crt", certPath);
        certsPaths.put("ca.key", keyPath);

        SecretUtils.createSecretFromFile(certsPaths, name, namespace, secretLabels);
        waitForSecretReady(name);
    }

    public static void waitForCertToChange(String originalCert, String secretName) {
        LOGGER.info("Waiting for Secret {} certificate change", secretName);
        TestUtils.waitFor("Cert to be replaced", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_CLUSTER_STABLE, () -> {
            Secret secret = kubeClient().getSecret(secretName);
            if (secret != null && secret.getData() != null && secret.getData().containsKey("ca.crt")) {
                String currentCert = new String(Base64.getDecoder().decode(secret.getData().get("ca.crt")), StandardCharsets.US_ASCII);
                boolean changed = !originalCert.equals(currentCert);
                if (changed) {
                    LOGGER.info("Certificate in Secret {} has changed, was {}, is now {}", secretName, originalCert, currentCert);
                }
                return changed;
            } else {
                return false;
            }
        });
    }

    public static void deleteSecretWithWait(String secretName, String namespace) {
        kubeClient().getClient().secrets().inNamespace(namespace).withName(secretName).delete();

        LOGGER.info("Waiting for Secret: {} to be deleted", secretName);
        TestUtils.waitFor(String.format("Deletion of secret: {}", secretName), Constants.GLOBAL_POLL_INTERVAL, DELETION_TIMEOUT,
            () -> kubeClient().getSecret(secretName) == null);

        LOGGER.info("Secret: {} successfully deleted", secretName);
    }
}
