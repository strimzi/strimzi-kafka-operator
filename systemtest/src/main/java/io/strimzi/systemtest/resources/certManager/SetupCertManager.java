/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.certManager;

import io.fabric8.certmanager.api.model.v1.Certificate;
import io.fabric8.certmanager.api.model.v1.CertificateBuilder;
import io.fabric8.certmanager.api.model.v1.CertificateList;
import io.fabric8.certmanager.api.model.v1.ClusterIssuer;
import io.fabric8.certmanager.api.model.v1.ClusterIssuerBuilder;
import io.fabric8.certmanager.api.model.v1.ClusterIssuerList;
import io.fabric8.kubernetes.api.model.Secret;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.skodjob.kubetest4j.resources.ResourceItem;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.utils.kubeUtils.NamespaceUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.NetworkPolicyUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
     * Suffix to use for the name of the Kubernetes {@code Secret} in the cert-manager namespace that holds the
     * CA certificate and private key, created by cert-manager from the CA Certificate resource.
     */
    private static final String CA_SECRET_SUFFIX = "-strimzi-ca-secret";

    /**
     * Suffix to use for the name of the {@code ClusterIssuer} that issues end-entity certificates using the CA.
     */
    private static final String CLUSTER_ISSUER_SUFFIX = "-strimzi-issuer";

    /**
     * Suffix to use for the name of the cert-manager {@code Certificate} resource that represents the CA.
     */
    private static final String CA_CERTIFICATE_SUFFIX = "-strimzi-ca";

    private static final String SELF_SIGNED_ISSUER_NAME = "selfsigned-bootstrap-issuer";

    private static final String CERT_MANAGER_DEPLOYMENT = "cert-manager";
    private static final String CERT_MANAGER_WEBHOOK_DEPLOYMENT = "cert-manager-webhook";
    private static final String CERT_MANAGER_CA_INJECTOR_DEPLOYMENT = "cert-manager-cainjector";

    private static final String CERT_MANAGER_PATH =
            TestUtils.USER_PATH + "/../systemtest/src/test/resources/cert-manager/cert-manager.yaml";

    private SetupCertManager() { }

    /**
     * Get CA Secret name for cluster
     * @param clusterName Cluster name
     * @return CA Secret name
     */
    public static String getCaSecretName(String clusterName) {
        return clusterName + CA_SECRET_SUFFIX;
    }

    /**
     * Get CA ClusterIssuer name for cluster
     * @param clusterName Cluster name
     * @return CA ClusterIssuer name
     */
    public static String getCaClusterIssuer(String clusterName) {
        return clusterName + CLUSTER_ISSUER_SUFFIX;
    }

    /**
     * Get CA Certificate name for cluster
     * @param clusterName Cluster name
     * @return CA Certificate name
     */
    public static String getCaCertificate(String clusterName) {
        return clusterName + CA_CERTIFICATE_SUFFIX;
    }

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
     * Creates a {@code SelfSigned} ClusterIssuer which is then used to bootstrap the CA.
     */
    public static void createSelfSignedIssuer() {
        LOGGER.info("Bootstrapping CA issuer chain via cert-manager");

        // SelfSigned ClusterIssuer (used only to bootstrap the CA certificate)
        final ClusterIssuer selfSignedIssuer = new ClusterIssuerBuilder()
                .withNewMetadata()
                    .withName(SELF_SIGNED_ISSUER_NAME)
                .endMetadata()
                .withNewSpec()
                    .withNewSelfSigned()
                    .endSelfSigned()
                .endSpec()
                .build();

        LOGGER.info("Creating SelfSigned ClusterIssuer '{}'", SELF_SIGNED_ISSUER_NAME);
        KubeResourceManager.get().kubeClient().getClient()
                .resources(ClusterIssuer.class, ClusterIssuerList.class)
                .resource(selfSignedIssuer).create();
        KubeResourceManager.get().pushToStack(new ResourceItem<>(() ->
                KubeResourceManager.get().kubeClient().getClient()
                        .resources(ClusterIssuer.class, ClusterIssuerList.class)
                        .withName(SELF_SIGNED_ISSUER_NAME).delete()));
    }

    /**
     * Bootstraps a CA issuer chain using cert-manager's own certificate lifecycle:
     * <ol>
     *   <li>A {@code Certificate} resource ({@code clusterName}{@value CA_CERTIFICATE_SUFFIX}) with {@code isCA=true}
     *       is created, referencing the SelfSigned issuer. cert-manager generates the CA
     *       certificate and private key and stores them in {@value CA_SECRET_SUFFIX}.</li>
     *   <li>A CA {@code ClusterIssuer} ({@code clusterName}{@value CLUSTER_ISSUER_SUFFIX}) is created, referencing
     *       the CA Secret so that cert-manager uses it to sign end-entity certificates.</li>
     * </ol>
     *
     * @param clusterName Cluster name to use as prefix for resources
     */
    public static void createIssuerAndCaSecret(String clusterName) {
        String caCertificateName = getCaCertificate(clusterName);
        String caSecretName = getCaSecretName(clusterName);
        String caClusterIssuerName = getCaClusterIssuer(clusterName);

        // Certificate resource for CA that will generate the CA cert+key in a Secret
        final Certificate caCertificate = new CertificateBuilder()
                .withNewMetadata()
                    .withName(caCertificateName)
                    .withNamespace(CERT_MANAGER_NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withIsCA(true)
                    .withCommonName("StrimziCA")
                    .withSecretName(caSecretName)
                    .withNewIssuerRef()
                        .withName(SELF_SIGNED_ISSUER_NAME)
                        .withKind("ClusterIssuer")
                        .withGroup("cert-manager.io")
                    .endIssuerRef()
                .endSpec()
                .build();

        LOGGER.info("Creating CA Certificate '{}' in namespace '{}'", caCertificateName, CERT_MANAGER_NAMESPACE);
        KubeResourceManager.get().kubeClient().getClient()
                .resources(Certificate.class, CertificateList.class)
                .inNamespace(CERT_MANAGER_NAMESPACE)
                .resource(caCertificate).create();
        KubeResourceManager.get().pushToStack(new ResourceItem<>(() ->
                KubeResourceManager.get().kubeClient().getClient()
                        .resources(Certificate.class, CertificateList.class)
                        .inNamespace(CERT_MANAGER_NAMESPACE)
                        .withName(caCertificateName).delete()));

        // Wait for cert-manager to create the CA Secret
        LOGGER.info("Waiting for cert-manager to create CA Secret '{}/{}'", CERT_MANAGER_NAMESPACE, caSecretName);
        TestUtils.waitFor("CA cert Secret to be created by cert-manager",
                TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
                () -> {
                    Secret s = KubeResourceManager.get().kubeClient().getClient()
                            .secrets().inNamespace(CERT_MANAGER_NAMESPACE).withName(caSecretName).get();
                    return s != null && s.getData() != null && s.getData().containsKey("tls.crt");
                });

        // ClusterIssuer that signs end-entity certificates using the bootstrapped CA
        final ClusterIssuer caIssuer = new ClusterIssuerBuilder()
                .withNewMetadata()
                    .withName(caClusterIssuerName)
                .endMetadata()
                .withNewSpec()
                    .withNewCa()
                        .withSecretName(caSecretName)
                    .endCa()
                .endSpec()
                .build();

        LOGGER.info("Creating ClusterIssuer '{}'", caClusterIssuerName);
        KubeResourceManager.get().kubeClient().getClient()
                .resources(ClusterIssuer.class, ClusterIssuerList.class)
                .resource(caIssuer).create();
        KubeResourceManager.get().pushToStack(new ResourceItem<>(() ->
                KubeResourceManager.get().kubeClient().getClient()
                        .resources(ClusterIssuer.class, ClusterIssuerList.class)
                        .withName(caClusterIssuerName).delete()));
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
                .secrets().inNamespace(CERT_MANAGER_NAMESPACE).withName(getCaSecretName(clusterName)).get();
        if (caSecret == null || caSecret.getData() == null || caSecret.getData().get("tls.crt") == null) {
            throw new IllegalStateException(
                    "CA Secret '" + getCaSecretName(clusterName) + "' not found in namespace '" + CERT_MANAGER_NAMESPACE +
                    "' — was createIssuerAndCaSecret() called first?");
        }
        return caSecret.getData().get("tls.crt");
    }
}
