/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import io.fabric8.kubernetes.api.model.Secret;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.systemtest.resources.certManager.SetupCertManager;

public class CertManagerUtils {
    /**
     * Get CA Secret name for cluster
     * @param clusterName Cluster name
     * @return CA Secret name
     */
    public static String getCaSecretName(String clusterName) {
        return clusterName + "-strimzi-ca-secret";
    }

    /**
     * Get CA ClusterIssuer name for cluster
     * @param clusterName Cluster name
     * @return CA ClusterIssuer name
     */
    public static String getCaClusterIssuer(String clusterName) {
        return clusterName + "-strimzi-issuer";
    }

    /**
     * Get CA Certificate name for cluster
     * @param clusterName Cluster name
     * @return CA Certificate name
     */
    public static String getCaCertificate(String clusterName) {
        return clusterName + "-strimzi-ca";
    }

    /**
     * Returns CA public certificate from the CA Secret for
     * {@code clusterName} cluster in the cert-manager namespace.
     *
     * @param clusterName Cluster name
     *
     * @return base64-encoded PEM certificate string (the {@code tls.crt} entry)
     * @throws IllegalStateException if the Secret or its {@code tls.crt} entry is absent
     */
    public static String getCaCertBase64(String clusterName) {
        final Secret caSecret = KubeResourceManager.get().kubeClient().getClient()
                .secrets().inNamespace(SetupCertManager.CERT_MANAGER_NAMESPACE).withName(getCaSecretName(clusterName)).get();
        if (caSecret == null || caSecret.getData() == null || caSecret.getData().get("tls.crt") == null) {
            throw new IllegalStateException(
                    "CA Secret '" + getCaSecretName(clusterName) + "' not found in namespace '" + SetupCertManager.CERT_MANAGER_NAMESPACE +
                    "' — was createIssuerAndCaSecret() called first?");
        }
        return caSecret.getData().get("tls.crt");
    }
}
