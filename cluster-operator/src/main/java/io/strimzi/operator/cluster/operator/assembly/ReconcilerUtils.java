/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.PodRevision;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.model.jmx.SupportsJmx;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.auth.PemAuthIdentity;
import io.strimzi.operator.common.auth.PemTrustSet;
import io.strimzi.operator.common.auth.Pkcs12AuthIdentity;
import io.strimzi.operator.common.auth.TlsIdentitySet;
import io.strimzi.operator.common.auth.TlsPemIdentity;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.ClientsCa;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.vertx.core.Future;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static io.strimzi.operator.common.Annotations.ANNO_STRIMZI_SERVER_CERT_HASH;

/**
 * Utilities used during reconciliation of different operands - mainly Kafka and ZooKeeper
 */
public class ReconcilerUtils {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ReconcilerUtils.class.getName());

    /**
     * In some cases, when the ClusterRoleBinding reconciliation fails with RBAC error and the desired object is null,
     * we want to ignore the error and return success. This is used to let Strimzi work without some Cluster-wide RBAC
     * rights when the features they are needed for are not used by the user.
     *
     * @param reconciliation    The reconciliation
     * @param reconcileFuture   The original reconciliation future
     * @param desired           The desired state of the resource.
     *
     * @return                  A future which completes when the resource was reconciled.
     */
    public static Future<ReconcileResult<ClusterRoleBinding>> withIgnoreRbacError(Reconciliation reconciliation, Future<ReconcileResult<ClusterRoleBinding>> reconcileFuture, ClusterRoleBinding desired) {
        return reconcileFuture.compose(
                rr -> Future.succeededFuture(),
                e -> {
                    if (desired == null
                            && e instanceof KubernetesClientException kce
                            && kce.getCode() == 403) {
                        LOGGER.debugCr(reconciliation, "Ignoring forbidden access to ClusterRoleBindings resource which does not seem to be required.");
                        return Future.succeededFuture();
                    }
                    return Future.failedFuture(e.getMessage());
                }
        );
    }

    /**
     * Waits for Pod readiness
     *
     * @param reconciliation        Reconciliation marker
     * @param podOperator           Pod operator
     * @param operationTimeoutMs    Operations timeout in milliseconds
     * @param podNames              List with the pod names which should be ready
     *
     * @return  Future which completes when all pods are ready or fails when they are not ready in time
     */
    public static Future<Void> podsReady(Reconciliation reconciliation, PodOperator podOperator, long operationTimeoutMs, List<String> podNames) {
        List<Future<Void>> podFutures = new ArrayList<>(podNames.size());

        for (String podName : podNames) {
            LOGGER.debugCr(reconciliation, "Checking readiness of pod {} in namespace {}", podName, reconciliation.namespace());
            podFutures.add(podOperator.readiness(reconciliation, reconciliation.namespace(), podName, 1_000, operationTimeoutMs));
        }

        return Future.join(podFutures)
                .map((Void) null);
    }

    /**
     * Utility method which helps to get the set of trusted certificates for the cluster CA needed to bootstrap different clients used during
     * the reconciliation.
     *
     * @param reconciliation Reconciliation Marker
     * @param secretOperator Secret operator for working with Kubernetes Secrets that store certificates
     *
     * @return  Future containing the trust set to use for client authentication.
     */
    public static Future<PemTrustSet> clusterCaPemTrustSet(Reconciliation reconciliation, SecretOperator secretOperator) {
        return getSecret(secretOperator, reconciliation.namespace(), KafkaResources.clusterCaCertificateSecretName(reconciliation.name()))
                .map(PemTrustSet::new);
    }

    /**
     * Utility method which helps to get the set of trusted certificates and client auth identities for the Cluster Operator
     * needed to bootstrap different clients used during the reconciliation.
     *
     * @param reconciliation Reconciliation Marker
     * @param secretOperator Secret operator for working with Kubernetes Secrets that store certificates
     *
     * @return  Future containing the TlsPemIdentity to use for client authentication.
     */
    public static Future<TlsPemIdentity> coTlsPemIdentity(Reconciliation reconciliation, SecretOperator secretOperator) {
        return Future.join(
                ReconcilerUtils.clusterCaPemTrustSet(reconciliation, secretOperator),
                coClientAuthIdentitySecret(reconciliation, secretOperator)
                        .map(PemAuthIdentity::clusterOperator)
        ).compose(res -> Future.succeededFuture(new TlsPemIdentity(res.resultAt(0), res.resultAt(1))));
    }

    /**
     * Utility method which helps to get the set of client auth identities for the Cluster Operator needed to bootstrap different
     * clients used during the reconciliation.
     *
     * @param reconciliation Reconciliation Marker
     * @param secretOperator Secret operator for working with Kubernetes Secrets that store certificates
     *
     * @return  Future containing a record with the Cluster Operator public and private key in both PEM and PKCS12 format
     */
    public static Future<TlsIdentitySet> coClientAuthIdentity(Reconciliation reconciliation, SecretOperator secretOperator) {
        return coClientAuthIdentitySecret(reconciliation, secretOperator)
                .map(secret -> new TlsIdentitySet(PemAuthIdentity.clusterOperator(secret), Pkcs12AuthIdentity.clusterOperator(secret)));
    }

    /**
     * Utility method which helps to get the Kubernetes Secret that contains the Cluster Operator identity to use for client authentication
     * when bootstrapping different clients used during the reconciliation.
     *
     * @param reconciliation Reconciliation Marker
     * @param secretOperator Secret operator for working with Kubernetes Secrets that store certificates
     *
     * @return  Future containing the Kubernetes Secret with the Cluster Operator public and private key in both PEM and PKCS12 format
     */
    private static Future<Secret> coClientAuthIdentitySecret(Reconciliation reconciliation, SecretOperator secretOperator) {
        return getSecret(secretOperator, reconciliation.namespace(), KafkaResources.secretName(reconciliation.name()));
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
    private static Future<Secret> getSecret(SecretOperator secretOperator, String namespace, String secretName)  {
        return secretOperator.getAsync(namespace, secretName).compose(secret -> {
            if (secret == null) {
                return Future.failedFuture(Util.missingSecretException(namespace, secretName));
            } else {
                return Future.succeededFuture(secret);
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
    public static RestartReasons reasonsToRestartPod(Reconciliation reconciliation, StrimziPodSet podSet, Pod pod, Set<String> fsResizingRestartRequest, boolean nodeCertsChange, Ca... cas) {
        RestartReasons restartReasons = RestartReasons.empty();

        if (pod == null)    {
            // When the Pod doesn't exist, it doesn't need to be restarted.
            // It will be created with new configuration.
            return restartReasons;
        }

        if (PodRevision.hasChanged(pod, podSet)) {
            restartReasons.add(RestartReason.POD_HAS_OLD_REVISION);
        }

        for (Ca ca: cas) {
            if (ca.certRenewed()) {
                restartReasons.add(RestartReason.CA_CERT_RENEWED, ca + " certificate renewal");
            }
            if (ca.certsRemoved()) {
                restartReasons.add(RestartReason.CA_CERT_REMOVED, ca + " certificate removal");
            }
            if (!isPodCaCertUpToDate(pod, ca)) {
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
     * Extracts and compares CA generation from the pod and from the CA class
     *
     * @param pod   Pod with the generation annotation
     * @param ca    Certificate Authority
     *
     * @return      True when the generations match, false otherwise
     */
    private static boolean isPodCaCertUpToDate(Pod pod, Ca ca) {
        return ca.caCertGeneration() == Annotations.intAnnotation(pod, getCaCertAnnotation(ca), Ca.INIT_GENERATION);
    }

    /**
     * Gets the right annotation for the CA generation depending on whether it is a Cluster or Clients CA
     *
     * @param ca    Certification Authority
     *
     * @return      Name of the annotation for given CA
     */
    private static String getCaCertAnnotation(Ca ca) {
        return ca instanceof ClientsCa ? Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION : Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION;
    }

    /**
     * Reconciles JMX Secret based on a JMX model
     *
     * @param reconciliation    Reconciliation marker
     * @param secretOperator    Operator for managing Secrets
     * @param cluster           Cluster which implements JMX support
     *
     * @return  Future which completes when the JMX Secret is reconciled
     */
    public static Future<Void> reconcileJmxSecret(Reconciliation reconciliation, SecretOperator secretOperator, SupportsJmx cluster)  {
        return secretOperator.getAsync(reconciliation.namespace(), cluster.jmx().secretName())
                .compose(currentJmxSecret -> {
                    Secret desiredJmxSecret = cluster.jmx().jmxSecret(currentJmxSecret);

                    if (desiredJmxSecret != null)  {
                        // Desired secret is not null => should be updated
                        return secretOperator.reconcile(reconciliation, reconciliation.namespace(), cluster.jmx().secretName(), desiredJmxSecret)
                                .map((Void) null);
                    } else if (currentJmxSecret != null)    {
                        // Desired secret is null but current is not => we should delete the secret
                        return secretOperator.reconcile(reconciliation, reconciliation.namespace(), cluster.jmx().secretName(), null)
                                .map((Void) null);
                    } else {
                        // Both current and desired secret are null => nothing to do
                        return Future.succeededFuture();
                    }
                });
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
     * Checks whether Node pools are enabled for given Kafka custom resource using the strimzi.io/node-pools annotation
     *
     * @param kafka     The Kafka custom resource which might have the node-pools annotation
     *
     * @return      True when the node pools are enabled. False otherwise.
     */
    public static boolean nodePoolsEnabled(Kafka kafka) {
        return KafkaCluster.ENABLED_VALUE_STRIMZI_IO_NODE_POOLS.equals(Annotations.stringAnnotation(kafka, Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "disabled").toLowerCase(Locale.ENGLISH));
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
}
