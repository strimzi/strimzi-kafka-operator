/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.operator.cluster.ClusterOperator;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.ClientsCa;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.operator.resource.PodRevision;
import io.strimzi.operator.cluster.operator.resource.StatefulSetOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
                            && e.getMessage() != null
                            && e.getMessage().contains("Message: Forbidden!")) {
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
        @SuppressWarnings({ "rawtypes" }) // Has to use Raw type because of the CompositeFuture
        List<Future> podFutures = new ArrayList<>(podNames.size());

        for (String podName : podNames) {
            LOGGER.debugCr(reconciliation, "Checking readiness of pod {} in namespace {}", podName, reconciliation.namespace());
            podFutures.add(podOperator.readiness(reconciliation, reconciliation.namespace(), podName, 1_000, operationTimeoutMs));
        }

        return CompositeFuture.join(podFutures)
                .map((Void) null);
    }

    /**
     * Utility method which helps to get the secrets with certificates needed to bootstrap different clients used during
     * the reconciliation.
     *
     * @param reconciliation Reconciliation Marker
     * @param secretOperator Secret operator for working with Secrets
     *
     * @return  Composite Future with the first result being the Kubernetes Secret with the Cluster CA and the second
     *          result being the Kubernetes Secret with the Cluster Operator public and private key.
     */
    public static CompositeFuture clientSecrets(Reconciliation reconciliation, SecretOperator secretOperator) {
        return CompositeFuture.join(
                getSecret(secretOperator, reconciliation.namespace(), KafkaResources.clusterCaCertificateSecretName(reconciliation.name())),
                getSecret(secretOperator, reconciliation.namespace(), ClusterOperator.secretName(reconciliation.name()))
        );
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
     * @param ctrlResource              Controller resource to which pod belongs
     * @param pod                       Pod to restart
     * @param fsResizingRestartRequest  Indicates which pods might need restart for filesystem resizing
     * @param nodeCertsChange           Indicates whether any certificates changed
     * @param cas                       Certificate authorities to be checked for changes
     *
     * @return empty RestartReasons if restart is not needed, non-empty RestartReasons otherwise
     */
    public static RestartReasons reasonsToRestartPod(Reconciliation reconciliation, HasMetadata ctrlResource, Pod pod, Set<String> fsResizingRestartRequest, boolean nodeCertsChange, Ca... cas) {
        RestartReasons restartReasons = RestartReasons.empty();

        if (pod == null)    {
            // When the Pod doesn't exist, it doesn't need to be restarted.
            // It will be created with new configuration.
            return restartReasons;
        }

        if (ctrlResource instanceof StatefulSet sts) {
            if (!isStatefulSetGenerationUpToDate(reconciliation, sts, pod)) {
                restartReasons.add(RestartReason.POD_HAS_OLD_GENERATION);
            }

            if (!isCustomCertUpToDate(reconciliation, sts, pod)) {
                restartReasons.add(RestartReason.CUSTOM_LISTENER_CA_CERT_CHANGE);
            }
        } else if (ctrlResource instanceof StrimziPodSet podSet) {
            if (PodRevision.hasChanged(pod, podSet)) {
                restartReasons.add(RestartReason.POD_HAS_OLD_REVISION);
            }
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
            LOGGER.debugCr(reconciliation, "Rolling pod {} due to {}",
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
        return ModelUtils.caCertGeneration(ca) == Annotations.intAnnotation(pod, getCaCertAnnotation(ca), Ca.INIT_GENERATION);
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
     * Checks whether the Pod and the StatefulSet have the same generation. If the generation differs, rolling update
     * will be needed.
     *
     * @param reconciliation    Reconciliation marker
     * @param sts               StatefulSet with the generation
     * @param pod               Pod with the Generation
     *
     * @return                  True if the generations match. False otherwise.
     */
    private static boolean isStatefulSetGenerationUpToDate(Reconciliation reconciliation, StatefulSet sts, Pod pod) {
        final int stsGeneration = StatefulSetOperator.getStsGeneration(sts);
        final int podGeneration = StatefulSetOperator.getPodGeneration(pod);

        LOGGER.debugCr(
                reconciliation,
                "Rolling update of {}/{}: pod {} has {}={}; sts has {}={}",
                sts.getMetadata().getNamespace(),
                sts.getMetadata().getName(),
                pod.getMetadata().getName(),
                StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION,
                podGeneration,
                StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION,
                stsGeneration
        );

        return stsGeneration == podGeneration;
    }

    /**
     * Checks whether custom certificate annotation is up-to-date.
     *
     * @param reconciliation    Reconciliation marker
     * @param sts               StatefulSet with the generation
     * @param pod               Pod with the Generation
     *
     * @return                  True if the generations match. False otherwise.
     */
    private static boolean isCustomCertUpToDate(Reconciliation reconciliation, StatefulSet sts, Pod pod) {
        final String stsThumbprint = Annotations.stringAnnotation(sts.getSpec().getTemplate(), KafkaCluster.ANNO_STRIMZI_CUSTOM_LISTENER_CERT_THUMBPRINTS, "");
        final String podThumbprint = Annotations.stringAnnotation(pod, KafkaCluster.ANNO_STRIMZI_CUSTOM_LISTENER_CERT_THUMBPRINTS, "");

        LOGGER.debugCr(
                reconciliation,
                "Rolling update of {}/{}: pod {} has {}={}; sts has {}={}",
                sts.getMetadata().getNamespace(),
                sts.getMetadata().getName(),
                pod.getMetadata().getName(),
                KafkaCluster.ANNO_STRIMZI_CUSTOM_LISTENER_CERT_THUMBPRINTS,
                podThumbprint,
                KafkaCluster.ANNO_STRIMZI_CUSTOM_LISTENER_CERT_THUMBPRINTS,
                stsThumbprint
        );

        return podThumbprint.equals(stsThumbprint);
    }
}
