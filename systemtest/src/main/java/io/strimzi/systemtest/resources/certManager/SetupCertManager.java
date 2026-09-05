/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.certManager;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.skodjob.kubetest4j.resources.ResourceItem;
import io.skodjob.kubetest4j.security.CertAndKey;
import io.skodjob.kubetest4j.security.CertAndKeyFiles;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.security.SystemTestCertGenerator;
import io.strimzi.systemtest.utils.kubeUtils.NamespaceUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.NetworkPolicyUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Base64;
import java.util.Map;

/**
 * Utility methods for installing and interacting with cert-manager in system tests.
 */
public class SetupCertManager {

    private static final Logger LOGGER = LogManager.getLogger(SetupCertManager.class);

    /**
     * Namespace the cert-manager will be deployed in
     */
    public static final String CERT_MANAGER_NAMESPACE = "cert-manager";

    /**
     * Name of the Kubernetes {@code Secret} in the cert-manager namespace that holds the
     * CA certificate and private key.
     */
    public static final String CA_SECRET_NAME = "strimzi-ca-secret";

    /**
     * Name of the {@code ClusterIssuer} to issue certificates.
     */
    public static final String CLUSTER_ISSUER_NAME = "strimzi-issuer";

    private static final String CERT_MANAGER_DEPLOYMENT = "cert-manager";
    private static final String CERT_MANAGER_WEBHOOK_DEPLOYMENT = "cert-manager-webhook";
    private static final String CERT_MANAGER_CA_INJECTOR_DEPLOYMENT = "cert-manager-cainjector";

    private static final String CERT_MANAGER_PATH =
            TestUtils.USER_PATH + "/../systemtest/src/test/resources/cert-manager/cert-manager.yaml";

    private static final String STRIMZI_ISSUER_PATH =
            TestUtils.USER_PATH + "/../systemtest/src/test/resources/cert-manager/strimzi-issuer.yaml";

    private static final String CERT_MANAGER_RBAC_DIR =
            TestUtils.USER_PATH + "/../packaging/install/cert-manager/";

    private SetupCertManager() { }

    /**
     * Deploys Cert Manager and adds it to the stack of resources to be deleted on clean up
     */
    public static void deployCertManager() {
        NamespaceUtils.createNamespaceAndPrepare(CERT_MANAGER_NAMESPACE);

        LOGGER.info("Deploying cert-manager from {}", CERT_MANAGER_PATH);
        KubeResourceManager.get().kubeCmdClient().apply(CERT_MANAGER_PATH);
        KubeResourceManager.get().pushToStack(new ResourceItem<>(SetupCertManager::deleteCertManager));

        waitForCertManagerReady();
        allowNetworkPolicyForWebhook();
    }

    /**
     * Deletes all Cert Manager resources and waits for their deletion
     */
    public static void deleteCertManager() {
        LOGGER.info("Deleting cert-manager");
        KubeResourceManager.get().kubeCmdClient().delete(CERT_MANAGER_PATH);
        DeploymentUtils.waitForDeploymentDeletion(CERT_MANAGER_NAMESPACE, CERT_MANAGER_DEPLOYMENT);
        DeploymentUtils.waitForDeploymentDeletion(CERT_MANAGER_NAMESPACE, CERT_MANAGER_WEBHOOK_DEPLOYMENT);
        DeploymentUtils.waitForDeploymentDeletion(CERT_MANAGER_NAMESPACE, CERT_MANAGER_CA_INJECTOR_DEPLOYMENT);
    }

    /**
     * Opens the {@link NetworkPolicyUtils} webhook rule so the cert-manager webhook
     * admission endpoint is reachable from within the cluster.
     */
    public static void allowNetworkPolicyForWebhook() {
        NetworkPolicyUtils.allowNetworkPolicySettingsForWebhook(
                CERT_MANAGER_NAMESPACE,
                CERT_MANAGER_DEPLOYMENT,
                Map.of(TestConstants.APP_KUBERNETES_INSTANCE_LABEL, CERT_MANAGER_DEPLOYMENT));
    }

    /**
     * Method that waits for all resources of Cert Manager to be up and running (ready) - Deployment, Webhook, and CA injector.
     * Also waits for the cainjector to finish injecting the CA bundle into the webhook configuration.
     */
    public static void waitForCertManagerReady() {
        DeploymentUtils.waitForDeploymentAndPodsReady(CERT_MANAGER_NAMESPACE, CERT_MANAGER_DEPLOYMENT, 1);
        DeploymentUtils.waitForDeploymentAndPodsReady(CERT_MANAGER_NAMESPACE, CERT_MANAGER_WEBHOOK_DEPLOYMENT, 1);
        DeploymentUtils.waitForDeploymentAndPodsReady(CERT_MANAGER_NAMESPACE, CERT_MANAGER_CA_INJECTOR_DEPLOYMENT, 1);

        // Wait for cainjector to populate the caBundle in the ValidatingWebhookConfiguration.
        // Until this is done, any POST to the cert-manager webhook (e.g. creating a Certificate CR)
        // fails with: tls: failed to verify certificate: x509: certificate signed by unknown authority
        LOGGER.info("Waiting for cert-manager cainjector to inject CA bundle into webhook configuration");
        TestUtils.waitFor("cert-manager webhook caBundle to be injected",
                TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
                () -> {
                    var webhookConfig = KubeResourceManager.get().kubeClient().getClient()
                            .admissionRegistration().v1().validatingWebhookConfigurations()
                            .withName("cert-manager-webhook")
                            .get();
                    if (webhookConfig == null || webhookConfig.getWebhooks() == null || webhookConfig.getWebhooks().isEmpty()) {
                        return false;
                    }
                    return webhookConfig.getWebhooks().stream()
                            .allMatch(w -> w.getClientConfig() != null
                                    && w.getClientConfig().getCaBundle() != null
                                    && !w.getClientConfig().getCaBundle().isEmpty());
                });

        LOGGER.info("cert-manager is ready in namespace '{}'", CERT_MANAGER_NAMESPACE);
    }

    /**
     * Creates ClusterIssuer that issues end-entity certificates signed by CA.
     *
     * <p>This simulates the steps a user would perform before deploying a Kafka cluster with
     * {@code clusterCa.type: cert-manager}. It creates self-signed CA public certificate
     * and private key and stores them in the Secret that would be used by ClusterIssuer.
     *
     * @return the subject DN of the generated CA certificate
     */
    public static String createIssuerAndCaSecret() {
        LOGGER.info("Generating self-signed CA certificate and key for cert-manager ClusterIssuer");

        final CertAndKey ca = SystemTestCertGenerator.generateRootCaCertAndKey();
        final String subjectDn = ca.getCertificate().getSubjectX500Principal().getName();
        final CertAndKeyFiles caFiles = SystemTestCertGenerator.exportToPemFiles(ca);

        final String certBase64;
        final String keyBase64;
        try {
            certBase64 = Base64.getEncoder().encodeToString(Files.readAllBytes(caFiles.certFile().toPath()));
            keyBase64 = Base64.getEncoder().encodeToString(Files.readAllBytes(caFiles.keyFile().toPath()));
        } catch (IOException e) {
            throw new RuntimeException("Failed to read CA cert/key PEM files", e);
        }

        // Create the Secret that the ClusterIssuer references
        final Secret caSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName(CA_SECRET_NAME)
                    .withNamespace(CERT_MANAGER_NAMESPACE)
                .endMetadata()
                .withType("kubernetes.io/tls")
                .addToData("tls.crt", certBase64)
                .addToData("tls.key", keyBase64)
                .build();

        LOGGER.info("Creating CA Secret '{}/{}' for ClusterIssuer", CERT_MANAGER_NAMESPACE, CA_SECRET_NAME);
        KubeResourceManager.get().kubeClient().getClient()
                .secrets().inNamespace(CERT_MANAGER_NAMESPACE).resource(caSecret).create();
        KubeResourceManager.get().pushToStack(new ResourceItem<>(() ->
                KubeResourceManager.get().kubeClient().getClient()
                        .secrets().inNamespace(CERT_MANAGER_NAMESPACE).withName(CA_SECRET_NAME).delete()));

        // Create the ClusterIssuer that signs end-entity certificates
        LOGGER.info("Creating ClusterIssuer '{}' from {}", CLUSTER_ISSUER_NAME, STRIMZI_ISSUER_PATH);
        KubeResourceManager.get().kubeCmdClient().apply(STRIMZI_ISSUER_PATH);
        KubeResourceManager.get().pushToStack(new ResourceItem<>(() ->
                KubeResourceManager.get().kubeCmdClient().delete(STRIMZI_ISSUER_PATH)));

        LOGGER.info("CA certificate subject DN: {}", subjectDn);
        return subjectDn;
    }

    /**
     * Returns CA public certificate from the {@value #CA_SECRET_NAME}
     * Secret in the cert-manager namespace.
     *
     * @return base64-encoded PEM certificate string (the {@code tls.crt} entry)
     * @throws IllegalStateException if the Secret or its {@code tls.crt} entry is absent
     */
    public static String getCaCertBase64() {
        final Secret caSecret = KubeResourceManager.get().kubeClient().getClient()
                .secrets().inNamespace(CERT_MANAGER_NAMESPACE).withName(CA_SECRET_NAME).get();
        if (caSecret == null || caSecret.getData() == null || caSecret.getData().get("tls.crt") == null) {
            throw new IllegalStateException(
                    "CA Secret '" + CA_SECRET_NAME + "' not found in namespace '" + CERT_MANAGER_NAMESPACE +
                    "' — was createIssuerAndCaSecret() called first?");
        }
        return caSecret.getData().get("tls.crt");
    }
}
