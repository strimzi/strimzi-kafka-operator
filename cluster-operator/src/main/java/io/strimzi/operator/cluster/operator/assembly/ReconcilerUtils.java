/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.api.kafka.model.common.CertAndKeySecretSource;
import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthentication;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationPlain;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationScram;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationTls;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.certs.CertAndKey;
import io.strimzi.operator.cluster.auth.RequestedServiceAccountAuthIdentity;
import io.strimzi.operator.cluster.model.InPlacePodResizingUtils;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.PodRevision;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.KafkaClusterSecurityContext;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.MtlsAuthenticationConfiguration;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.ServiceAccountAuthenticationConfiguration;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.TlsEncryptionConfiguration;
import io.strimzi.operator.cluster.model.jmx.SupportsJmx;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.auth.AuthIdentity;
import io.strimzi.operator.common.auth.Identity;
import io.strimzi.operator.common.auth.PemAuthIdentity;
import io.strimzi.operator.common.auth.PemTrustSet;
import io.strimzi.operator.common.auth.TrustSet;
import io.strimzi.operator.common.ca.Ca;
import io.strimzi.operator.common.ca.CertificateUtils;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.kubernetes.SecretOperator;

import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import static io.strimzi.operator.common.Annotations.ANNO_STRIMZI_SERVER_CERT_HASH;

/**
 * Utilities used during reconciliation of different operands
 */
public class ReconcilerUtils {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ReconcilerUtils.class.getName());

    private ReconcilerUtils() { }

    /**
     * In some cases, when the ClusterRoleBinding reconciliation fails with RBAC error and the desired object is null,
     * we want to ignore the error and return success. This is used to let Strimzi work without some Cluster-wide RBAC
     * rights when the features they are needed for are not used by the user.
     *
     * @param reconciliation    The reconciliation
     * @param reconcileFuture   The original reconciliation future
     * @param desired           The desired state of the resource.
     *
     * @return                  A CompletionStage which completes when the resource was reconciled.
     */
    public static CompletionStage<ReconcileResult<ClusterRoleBinding>> withIgnoreRbacError(Reconciliation reconciliation, CompletionStage<ReconcileResult<ClusterRoleBinding>> reconcileFuture, ClusterRoleBinding desired) {
        return reconcileFuture.handle((rr, e) -> {
            if (e == null) {
                return rr;
            }
            // The resource operators run on a CompletableFuture, which wraps exceptions thrown by the
            // Kubernetes client in a CompletionException. We therefore unwrap it before checking the type and
            // status code (a forbidden ClusterRoleBindings access which is not required is ignored).
            if (desired == null && Util.maybeUnwrapCompletionException(e) instanceof KubernetesClientException kce
                    && kce.getCode() == 403) {
                LOGGER.debugCr(reconciliation, "Ignoring forbidden access to ClusterRoleBindings resource which does not seem to be required.");
                return null;
            }
            var throwable = Util.maybeUnwrapCompletionException(e);
            throw throwable instanceof RuntimeException re ? re : new RuntimeException(throwable);
        });
    }

    /**
     * Waits for Pod readiness
     *
     * @param reconciliation        Reconciliation marker
     * @param podOperator           Pod operator
     * @param operationTimeoutMs    Operations timeout in milliseconds
     * @param podNames              List with the pod names which should be ready
     *
     * @return  CompletionStage which completes when all pods are ready or fails when they are not ready in time
     */
    public static CompletionStage<Void> podsReady(Reconciliation reconciliation, PodOperator podOperator, long operationTimeoutMs, List<String> podNames) {
        List<CompletableFuture<Void>> podFutures = new ArrayList<>(podNames.size());

        for (String podName : podNames) {
            LOGGER.debugCr(reconciliation, "Checking readiness of pod {} in namespace {}", podName, reconciliation.namespace());
            podFutures.add(podOperator.readiness(reconciliation, reconciliation.namespace(), podName, 1_000, operationTimeoutMs).toCompletableFuture());
        }

        return CompletableFuture.allOf(podFutures.toArray(new CompletableFuture[0]));
    }

    /**
     * Utility method which helps to get the set of trusted certificates and client auth identities for the Cluster Operator
     * needed to bootstrap different clients used during the reconciliation.
     *
     * @param reconciliation    Reconciliation Marker
     * @param secretOperator    Secret operator for working with Kubernetes Secrets that store certificates
     * @param securityContext   Kafka cluster security context
     *
     * @return  CompletionStage containing the PemTrustSet and PemAuthIdentity to use for client authentication.
     */
    @SuppressFBWarnings("DLS_DEAD_LOCAL_STORE") // SpotBugs does not like the unused `ignored` binding in the switch pattern
    public static CompletionStage<Identity> coIdentity(Reconciliation reconciliation, SecretOperator secretOperator, KafkaClusterSecurityContext securityContext) {
        // Gets the trust based on the security context
        CompletionStage<TrustSet> trustFuture = securityContext.encryption() instanceof TlsEncryptionConfiguration ?
                clusterCaPemTrustSet(reconciliation, secretOperator, securityContext).thenApply(t -> (TrustSet) t) : CompletableFuture.completedFuture(null);
        return trustFuture
                .thenCompose(trustSet -> {
                    // Gets the identity based on the security context
                    CompletionStage<AuthIdentity> authFuture = switch (securityContext.authentication()) {
                        case MtlsAuthenticationConfiguration ignored -> coPemAuthIdentity(reconciliation, secretOperator).thenApply(a -> (AuthIdentity) a);
                        case ServiceAccountAuthenticationConfiguration sa -> CompletableFuture.completedFuture((AuthIdentity) new RequestedServiceAccountAuthIdentity(reconciliation, sa.audience(), sa.expirationSeconds()));
                        default -> CompletableFuture.completedFuture(null);
                    };

                    return authFuture.thenApply(r -> new Identity(trustSet, r));
                });
    }

    /**
     * Utility method which helps to get the set of trusted certificates for the cluster CA needed to bootstrap different clients used during
     * the reconciliation.
     *
     * @param reconciliation Reconciliation Marker
     * @param secretOperator Secret operator for working with Kubernetes Secrets that store certificates
     *
     * @return  CompletionStage containing the trust set to use for client authentication.
     */
    private static CompletionStage<PemTrustSet> clusterCaPemTrustSet(Reconciliation reconciliation, SecretOperator secretOperator, KafkaClusterSecurityContext securityContext) {
        return getSecret(secretOperator, reconciliation.namespace(), KafkaResources.clusterCaCertificateSecretName(reconciliation.name()))
                .thenApply(PemTrustSet::new);
    }

    /**
     * Utility method which helps to get the Cluster Operator identity to use for client authentication
     * when bootstrapping different clients used during the reconciliation.
     *
     * @param reconciliation Reconciliation Marker
     * @param secretOperator Secret operator for working with Kubernetes Secrets that store certificates
     *
     * @return  CompletionStage containing the auth identity to use for client authentication.
     */
    private static CompletionStage<PemAuthIdentity> coPemAuthIdentity(Reconciliation reconciliation, SecretOperator secretOperator) {
        return getSecret(secretOperator, reconciliation.namespace(), KafkaResources.clusterOperatorCertsSecretName(reconciliation.name()))
                .thenApply(PemAuthIdentity::clusterOperator);
    }

    /**
     * Gets asynchronously a secret. If it doesn't exist, it throws exception.
     *
     * @param secretOperator    Operator for getting the secrets
     * @param namespace         Namespace where the secret is
     * @param secretName        Name of the secret
     *
     * @return                  Secret with the certificates
     */
    private static CompletionStage<Secret> getSecret(SecretOperator secretOperator, String namespace, String secretName)  {
        return secretOperator.getAsync(namespace, secretName).thenCompose(secret -> {
            if (secret == null) {
                return CompletableFuture.failedFuture(missingSecretException(namespace, secretName));
            } else {
                return CompletableFuture.completedFuture(secret);
            }
        });
    }

    /**
     * Determines if the Pod needs to be rolled / restarted. If it does, returns a list of reasons why.
     *
     * @param reconciliation            Reconciliation Marker
     * @param podSet                    StrimziPodSet to which pod belongs
     * @param pod                       Pod to restart
     * @param fsResizingRestartRequest  Indicates which pods might need restart for filesystem resizing
     * @param nodeCertsChange           Indicates whether any certificates changed
     * @param cas                       Certificate authorities to be checked for changes
     *
     * @return empty RestartReasons if restart is not needed, non-empty RestartReasons otherwise
     */
    @SuppressWarnings("NPathComplexity")
    public static RestartReasons reasonsToRestartPod(Reconciliation reconciliation, StrimziPodSet podSet, Pod pod, Set<String> fsResizingRestartRequest, boolean nodeCertsChange, Ca... cas) {
        RestartReasons restartReasons = RestartReasons.empty();

        if (pod == null)    {
            // When the Pod doesn't exist, it doesn't need to be restarted.
            // It will be created with new configuration.
            return restartReasons;
        }

        if (PodRevision.hasChanged(pod, podSet, PodRevision.STRIMZI_REVISION_ANNOTATION)) {
            restartReasons.add(RestartReason.POD_HAS_OLD_REVISION);
        }

        // Adds any reasons to restart related to in-place Pod resizing
        InPlacePodResizingUtils.reasonsToRestart(reconciliation, restartReasons, podSet, pod);

        for (Ca ca: cas) {
            if (ca.certRenewed()) {
                restartReasons.add(RestartReason.CA_CERT_RENEWED, ca + " certificate renewal");
            }
            if (ca.certsRemoved()) {
                restartReasons.add(RestartReason.CA_CERT_REMOVED, ca + " certificate removal");
            }
            if (ca.hasCaCertGenerationChanged(pod)) {
                restartReasons.add(RestartReason.CA_CERT_HAS_OLD_GENERATION, "Pod has old " + ca + " certificate generation");
            }
        }

        if (fsResizingRestartRequest.contains(pod.getMetadata().getName()))   {
            restartReasons.add(RestartReason.FILE_SYSTEM_RESIZE_NEEDED);
        }

        if (nodeCertsChange) {
            restartReasons.add(RestartReason.KAFKA_CERTIFICATES_CHANGED);
        }

        if (restartReasons.shouldRestart()) {
            LOGGER.debugCr(reconciliation, "Rolling Pod {} due to {}",
                    pod.getMetadata().getName(), restartReasons.getAllReasonNotes());
        }

        return restartReasons;
    }

    /**
     * Reconciles JMX Secret based on a JMX model
     *
     * @param reconciliation    Reconciliation marker
     * @param secretOperator    Operator for managing Secrets
     * @param cluster           Cluster which implements JMX support
     *
     * @return  CompletionStage which completes when the JMX Secret is reconciled
     */
    public static CompletionStage<Void> reconcileJmxSecret(Reconciliation reconciliation, SecretOperator secretOperator, SupportsJmx cluster)  {
        return secretOperator.getAsync(reconciliation.namespace(), cluster.jmx().secretName())
                .thenCompose(currentJmxSecret -> {
                    Secret desiredJmxSecret = cluster.jmx().jmxSecret(currentJmxSecret);

                    if (desiredJmxSecret != null)  {
                        // Desired secret is not null => should be updated
                        return secretOperator.reconcile(reconciliation, reconciliation.namespace(), cluster.jmx().secretName(), desiredJmxSecret);
                    } else if (currentJmxSecret != null)    {
                        // Desired secret is null but current is not => we should delete the secret
                        return secretOperator.reconcile(reconciliation, reconciliation.namespace(), cluster.jmx().secretName(), null);
                    } else {
                        // Both current and desired secret are null => nothing to do
                        return CompletableFuture.completedFuture(null);
                    }
                }).thenApply(ignored -> null);
    }

    /**
     * Utility method to extract pod index number from pod name
     *
     * @param podName   Name of the pod
     *
     * @return          Index of the pod
     */
    public static int getPodIndexFromPodName(String podName)  {
        return Integer.parseInt(podName.substring(podName.lastIndexOf("-") + 1));
    }

    /**
     * Utility method to extract controller name from pod name (= the part before the index at the end)
     *
     * @param podName   Name of the pod
     *
     * @return          Name of the controller
     */
    public static String getControllerNameFromPodName(String podName)  {
        return podName.substring(0, podName.lastIndexOf("-"));
    }

    /**
     * Gets a list of node references from a PodSet
     *
     * @param podSet    The PodSet from which the nodes should extracted
     *
     * @return  List of node references based on this PodSet
     */
    public static List<NodeRef> nodesFromPodSet(StrimziPodSet podSet)   {
        return PodSetUtils
                .podSetToPods(podSet)
                .stream()
                .map(pod -> nodeFromPod(pod))
                .toList();
    }

    /**
     * Creates a node reference from a Pod
     *
     * @param pod   Pod from which the node reference should be created
     *
     * @return  Node reference for this pod
     */
    public static NodeRef nodeFromPod(Pod pod) {
        return new NodeRef(
                pod.getMetadata().getName(),
                ReconcilerUtils.getPodIndexFromPodName(pod.getMetadata().getName()),
                ReconcilerUtils.getPoolNameFromPodName(clusterNameFromLabel(pod), pod.getMetadata().getName()),
                hasRole(pod, Labels.STRIMZI_CONTROLLER_ROLE_LABEL),
                hasRole(pod, Labels.STRIMZI_BROKER_ROLE_LABEL));
    }

    /**
     * Utility method to extract pool name from pod name. The Pod name consists from 3 parts: the cluster name, the pod
     * suffix / index and the pool name in the middle. So when we know the cluster name, we can extract the pool name
     * from it.
     *
     * @param clusterName   Name of the cluster
     * @param podName       Name of the pod
     *
     * @return          Name of the pool
     */
    /* test */ static String getPoolNameFromPodName(String clusterName, String podName)  {
        return podName.substring(clusterName.length() + 1, podName.lastIndexOf("-"));
    }

    /**
     * Extracts cluster name from the strimzi.io/cluster labels
     *
     * @param resource  The resource with labels
     *
     * @return  The name of the cluster
     *
     * @throws  RuntimeException is thrown when the label is missing
     */
    /* test */ static String clusterNameFromLabel(HasMetadata resource)    {
        String clusterName = null;

        if (resource != null
                && resource.getMetadata() != null
                && resource.getMetadata().getLabels() != null) {
            clusterName = resource.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL);
        }

        if (clusterName != null)    {
            return clusterName;
        } else {
            throw new RuntimeException("Failed to extract cluster name from Pod label");
        }
    }

    /**
     * Checks if the Kubernetes resource has the given label and if it does, whether it contains "true". In that case,
     * it will return true. If the label is not set or does not contain "true", it will return false.
     *
     * @param resource  Resource where to check for the label
     * @param label     Name of the label (e.g. strimzi.io/controller-role or strimzi.io/broker-role)
     *
     * @return  True if the label is present and set to "true". False otherwise.
     */
    private static boolean hasRole(HasMetadata resource, String label)    {
        if (resource != null
                && resource.getMetadata() != null
                && resource.getMetadata().getLabels() != null) {
            return "true".equals(resource.getMetadata().getLabels().getOrDefault(label, "false"));
        } else {
            return false;
        }
    }

    /**
     * Check weather the pod is tracking an outdated server certificate.
     *
     * Returns false if the pod isn't tracking a server certificate (i.e. isn't annotated with ANNO_STRIMZI_SERVER_CERT_HASH)
     *
     * @param pod Pod resource that's possibly tracking a server certificate.
     * @param certHashCache An up-to-date server cache which maps pod index to the certificate hash
     * @return True if pod tracks a server certificate, the certificate hash is cached, and the tracked hash differs from the cached one
     */
    public static boolean trackedServerCertChanged(Pod pod, Map<Integer, String> certHashCache) {
        var currentCertHash = Annotations.stringAnnotation(pod, ANNO_STRIMZI_SERVER_CERT_HASH, null);
        var desiredCertHash = certHashCache.get(ReconcilerUtils.getPodIndexFromPodName(pod.getMetadata().getName()));
        return currentCertHash != null && desiredCertHash != null && !currentCertHash.equals(desiredCertHash);
    }

    /**
     * Creates a hash from Secret's content.
     * 
     * @param secret Secret with content.
     * @return Hash of the secret content.
     */
    public static String hashSecretContent(Secret secret) {
        if (secret == null) {
            throw new RuntimeException("Secret not found");
        }
        
        if (secret.getData() == null || secret.getData().isEmpty()) {
            throw new RuntimeException("Empty secret");
        }
        
        StringBuilder sb = new StringBuilder();
        secret.getData().entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(entry -> sb.append(entry.getKey()).append(entry.getValue()));
        
        return Util.hashStub(sb.toString());
    }

    /**
     * Returns exception when secret is missing. This is used from several different methods to provide identical exception.
     *
     * @param namespace     Namespace of the Secret
     * @param secretName    Name of the Secret
     * @return              RuntimeException
     */
    static RuntimeException missingSecretException(String namespace, String secretName) {
        return new RuntimeException("Secret " + namespace + "/" + secretName + " does not exist");
    }

    /**
     * Checks if the Kubernetes resource matches LabelSelector. This is useful when you use get/getAsync to retrieve a
     * resource and want to check if it matches the labels from the selector (since get/getAsync is using name and not
     * labels to identify the resource). This method currently supports only the matchLabels object. matchExpressions
     * array is not supported.
     *
     * @param labelSelector The LabelSelector with the labels which should be present in the resource
     * @param cr            The Custom Resource which labels should be checked
     *
     * @return              True if the resource contains all labels from the LabelSelector or if the LabelSelector is empty
     */
    /* test */ static boolean matchesSelector(LabelSelector labelSelector, HasMetadata cr) {
        if (labelSelector != null && labelSelector.getMatchLabels() != null) {
            if (cr.getMetadata().getLabels() != null) {
                return cr.getMetadata().getLabels().entrySet().containsAll(labelSelector.getMatchLabels().entrySet());
            } else {
                return labelSelector.getMatchLabels().isEmpty();
            }
        }

        return true;
    }

    /**
     * When TLS certificate or Auth certificate (or password) is changed, the hash is computed.
     * It is used for rolling updates.
     * @param secretOperations Secret operator
     * @param namespace namespace to get Secrets in
     * @param auth Authentication object to compute hash from
     * @param certs certificates list to compute hash from.
     *
     * @return CompletionStage computing hash from TLS + Auth
     */
    public static CompletionStage<Integer> authTlsHash(SecretOperator secretOperations, String namespace, KafkaClientAuthentication auth, Collection<String> certs) {
        CompletionStage<Integer> tlsFuture;
        if (certs == null || certs.isEmpty()) {
            tlsFuture = CompletableFuture.completedFuture(0);
        } else {
            // get all TLS trusted certs, compute hash from them
            tlsFuture = CompletableFuture.completedFuture(certs.stream().mapToInt(String::hashCode).sum());
        }

        if (auth == null) {
            return tlsFuture;
        } else {
            // compute hash from Auth
            if (auth instanceof KafkaClientAuthenticationScram) {
                // only passwordSecret can be changed
                return tlsFuture.thenCompose(tlsHash -> getPasswordAsync(secretOperations, namespace, auth)
                        .thenCompose(password -> CompletableFuture.completedFuture(password.hashCode() + tlsHash)));
            } else if (auth instanceof KafkaClientAuthenticationPlain) {
                // only passwordSecret can be changed
                return tlsFuture.thenCompose(tlsHash -> getPasswordAsync(secretOperations, namespace, auth)
                        .thenCompose(password -> CompletableFuture.completedFuture(password.hashCode() + tlsHash)));
            } else if (auth instanceof KafkaClientAuthenticationTls authTls) {
                // custom cert can be used (and changed)
                return authTls.getCertificateAndKey() == null ? tlsFuture : tlsFuture.thenCompose(tlsHash -> getCertificateAndKeyAsync(secretOperations, namespace, authTls.getCertificateAndKey())
                        .thenCompose(crtAndKey -> CompletableFuture.completedFuture(crtAndKey.certAsBase64String().hashCode() + crtAndKey.keyAsBase64String().hashCode() + tlsHash)));
            } else {
                // unknown Auth type
                return tlsFuture;
            }
        }
    }

    /**
     * Gets trusted certificates from Secrets and merges them into a single List of Strings.
     *
     * @param reconciliation        Reconciliation marker
     * @param secretOperations      Secrets operator
     * @param certificateSources    List of certificate sources
     *
     * @return  Certificates extracted from the Secrets
     */
    public static CompletionStage<List<String>> trustedCertificates(Reconciliation reconciliation, SecretOperator secretOperations, List<CertSecretSource> certificateSources)   {
        if (certificateSources != null && !certificateSources.isEmpty()) {
            List<CompletableFuture<String>> futures = certificateSources
                    .stream()
                    .map(certSecretSource -> ReconcilerUtils.getTrustedCertificateAsync(secretOperations, reconciliation.namespace(), certSecretSource).toCompletableFuture())
                    .toList();
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .thenApply(v -> futures.stream().map(CompletableFuture::join).toList());
        } else {
            // No trusted certificates to extract.
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Utility method which gets the secret and validates that the required fields are present in it
     *
     * @param secretOperator    Secret operator to get the secret from the Kubernetes API
     * @param namespace         Namespace where the Secret exist
     * @param name              Name of the Secret
     * @param items             List of items which should be present in the Secret
     *
     * @return      CompletionStage with the Secret if is exits and has the required items. Failed future with an error message otherwise.
     */
    /* test */ static CompletionStage<Secret> getValidatedSecret(SecretOperator secretOperator, String namespace, String name, String... items) {
        return secretOperator.getAsync(namespace, name)
                .thenCompose(secret -> validatedSecret(namespace, name, secret, items));
    }

    /**
     * Utility method which validates that the required fields are present in a Secret passed to it
     *
     * @param namespace         Namespace of the Secret
     * @param name              Name of the Secret (used for error message in case the Secret is null)
     * @param secret            Secret that should be validated or null if the Secret does not exist
     * @param items             List of items which should be present in the Secret
     *
     * @return      CompletionStage with the Secret if is exits and has the required items. Failed future with an error message otherwise.
     */
    private static CompletionStage<Secret> validatedSecret(String namespace, String name, Secret secret, String... items) {
        if (secret == null) {
            return CompletableFuture.failedFuture(new InvalidConfigurationException("Secret " + name + " not found in namespace " + namespace));
        } else {
            List<String> errors = new ArrayList<>(0);

            if (items != null) {
                for (String item : items) {
                    if (!secret.getData().containsKey(item)) {
                        // Item not found => error will be raised
                        errors.add(item);
                    }
                }
            }

            if (errors.isEmpty()) {
                return CompletableFuture.completedFuture(secret);
            } else {
                return CompletableFuture.failedFuture(new InvalidConfigurationException(String.format("Items with key(s) %s are missing in Secret %s", errors, name)));
            }
        }
    }

    private static CompletionStage<String> getTrustedCertificateAsync(SecretOperator secretOperator, String namespace, CertSecretSource certSecretSource) {
        return secretOperator.getAsync(namespace, certSecretSource.getSecretName())
                .thenCompose(secret -> {
                    if (certSecretSource.getCertificate() != null)  {
                        return validatedSecret(namespace, certSecretSource.getSecretName(), secret, certSecretSource.getCertificate())
                                .thenCompose(validatedSecret -> {
                                    try {
                                        String pem = CertificateUtils.x509CertificateToPem(CertificateUtils.x509Certificate(Util.decodeFromBase64(validatedSecret.getData().get(certSecretSource.getCertificate())).getBytes(StandardCharsets.US_ASCII)));
                                        return CompletableFuture.completedFuture(pem);
                                    } catch (CertificateException e) {
                                        throw new RuntimeException("Failed to load certificate from Secret " + certSecretSource.getSecretName() + " from namespace " + namespace, e);
                                    }
                                });
                    } else if (certSecretSource.getPattern() != null)    {
                        return validatedSecret(namespace, certSecretSource.getSecretName(), secret)
                                .thenCompose(validatedSecret -> {
                                    PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + certSecretSource.getPattern());

                                    String pems = validatedSecret
                                            .getData()
                                            .entrySet()
                                            .stream()
                                            .filter(entry -> matcher.matches(Paths.get(entry.getKey())))
                                            .map(entry -> {
                                                try {
                                                    return CertificateUtils.x509CertificateToPem(CertificateUtils.x509Certificate(Util.decodeFromBase64(validatedSecret.getData().get(entry.getKey())).getBytes(StandardCharsets.US_ASCII)));
                                                } catch (CertificateException e) {
                                                    throw new RuntimeException("Failed to load certificate from Secret " + certSecretSource.getSecretName() + " from namespace " + namespace, e);
                                                }
                                            })
                                            .sorted()
                                            .collect(Collectors.joining("\n"));
                                    return CompletableFuture.completedFuture(pems);
                                });
                    } else {
                        throw new InvalidResourceException("Certificate source does not contain the certificate or the pattern.");
                    }
                });
    }

    /**
     * Extracts certificate and key from the given Secret source.
     *
     * @param secretOperator            Secrets operator
     * @param namespace                 namespace to get Secret in
     * @param certAndKeySecretSource    Secret source for certificate and key
     * @return  Certificate and Key extracted from the Secret source and decoded
     */
    public static CompletionStage<CertAndKey> getCertificateAndKeyAsync(SecretOperator secretOperator, String namespace, CertAndKeySecretSource certAndKeySecretSource) {
        return getValidatedSecret(secretOperator, namespace, certAndKeySecretSource.getSecretName(), certAndKeySecretSource.getCertificate(), certAndKeySecretSource.getKey())
                .thenCompose(secret -> CompletableFuture.completedFuture(new CertAndKey(
                        Util.decodeBytesFromBase64(secret.getData().get(certAndKeySecretSource.getKey())),
                        Util.decodeBytesFromBase64(secret.getData().get(certAndKeySecretSource.getCertificate())))));
    }

    private static CompletionStage<String> getPasswordAsync(SecretOperator secretOperator, String namespace, KafkaClientAuthentication auth) {
        if (auth instanceof KafkaClientAuthenticationPlain plainAuth) {

            return getValidatedSecret(secretOperator, namespace, plainAuth.getPasswordSecret().getSecretName(), plainAuth.getPasswordSecret().getPassword())
                    .thenCompose(secret -> CompletableFuture.completedFuture(secret.getData().get(plainAuth.getPasswordSecret().getPassword())));
        } else if (auth instanceof KafkaClientAuthenticationScram scramAuth) {

            return getValidatedSecret(secretOperator, namespace, scramAuth.getPasswordSecret().getSecretName(), scramAuth.getPasswordSecret().getPassword())
                    .thenCompose(secret -> CompletableFuture.completedFuture(secret.getData().get(scramAuth.getPasswordSecret().getPassword())));
        } else {
            return CompletableFuture.failedFuture(new RuntimeException("Auth type " + auth.getType() + " does not have a password property"));
        }
    }
}
