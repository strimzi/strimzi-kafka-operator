/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.StrimziPodSetBuilder;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.StatefulSetOperator;
import io.strimzi.operator.common.operator.resource.StrimziPodSetOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.AbstractScalableNamespacedResourceOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.PvcOperator;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class takes care of manually cleaning pods belonging to a specific controller if it is requested by the user
 * using an annotation. Pod clean-up in this case means deletion of the pod and related PVCs.
 */
public class ManualPodCleaner {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ManualPodCleaner.class.getName());

    private final Reconciliation reconciliation;
    private final String controllerResourceName;
    private final Labels selector;
    private final long operationTimeoutMs;
    private final boolean useStrimziPodSets;

    private final PodOperator podOperator;
    private final PvcOperator pvcOperator;
    private final StatefulSetOperator stsOperator;
    private final StrimziPodSetOperator strimziPodSetOperator;

    /**
     * Constructs the Manual Pod Cleaner.
     *
     * @param reconciliation    Reconciliation marker
     * @param ctrlResourceName  Name of the controller resource (e.g. StatefulSet)
     * @param selector          Selector for selecting the Pods belonging to this controller
     * @param config            Cluster Operator Configuration
     * @param supplier          Resource Operator Supplier with the Kubernetes resource operators
     */
    public ManualPodCleaner(
            Reconciliation reconciliation,
            String ctrlResourceName,
            Labels selector,
            ClusterOperatorConfig config,
            ResourceOperatorSupplier supplier
    ) {
        this.reconciliation = reconciliation;
        this.controllerResourceName = ctrlResourceName;
        this.selector = selector;
        this.operationTimeoutMs = config.getOperationTimeoutMs();
        this.useStrimziPodSets = config.featureGates().useStrimziPodSetsEnabled();

        this.stsOperator = supplier.stsOperations;
        this.strimziPodSetOperator = supplier.strimziPodSetOperator;
        this.pvcOperator = supplier.pvcOperations;
        this.podOperator = supplier.podOperations;
    }

    /**
     * Constructs the Manual Pod Cleaner
     *
     * @param reconciliation        Reconciliation marker
     * @param ctrlResourceName      Name of the controller resource (e.g. StatefulSet)
     * @param selector              Selector for selecting the Pods belonging to this controller
     * @param operationTimeoutMs    Timeout for Kubernetes operations
     * @param useStrimziPodSets     Flag indicating whether StrimziPodSets are being used
     * @param stsOperator           The StatefulSet operator for working with Kubernetes StatefulSet
     * @param strimziPodSetOperator The Pod Set operator for working with Strimzi Pod Sets
     * @param podOperator           The Pod operator for working with Kubernetes Pods
     * @param pvcOperator           The Persistent Volume Claim operator for working with Kubernetes PVC
     */
    public ManualPodCleaner(
            Reconciliation reconciliation,
            String ctrlResourceName,
            Labels selector,
            long operationTimeoutMs,
            boolean useStrimziPodSets,

            StatefulSetOperator stsOperator,
            StrimziPodSetOperator strimziPodSetOperator,
            PodOperator podOperator,
            PvcOperator pvcOperator
    ) {
        this.reconciliation = reconciliation;
        this.controllerResourceName = ctrlResourceName;
        this.selector = selector;
        this.operationTimeoutMs = operationTimeoutMs;
        this.useStrimziPodSets = useStrimziPodSets;

        this.stsOperator = stsOperator;
        this.strimziPodSetOperator = strimziPodSetOperator;
        this.pvcOperator = pvcOperator;
        this.podOperator = podOperator;
    }

    /**
     * Checks pods belonging to a single controller (e.g. StatefulSet) cluster to see whether the user requested
     * them to be deleted including their PVCs. If the user requested it, it will delete them. In a single
     * reconciliation, always only one Pod is deleted. If multiple pods are marked for cleanup, they will be done
     * in subsequent reconciliations. This method only checks if cleanup was requested and calls other methods to
     * execute it.
     *
     * @param desiredPvcs   The list of desired PVCs which should be created after the old Pod and PVCs are deleted
     *
     * @return              Future indicating the result of the cleanup operation. Returns always success if there are no pods to clean.
     */
    public Future<Void> maybeManualPodCleaning(List<PersistentVolumeClaim> desiredPvcs) {
        return podOperator.listAsync(reconciliation.namespace(), selector)
                .compose(pods -> {
                    // Only one pod per reconciliation is rolled
                    Pod podToClean = pods
                            .stream()
                            .filter(pod -> Annotations.booleanAnnotation(pod, AbstractScalableNamespacedResourceOperator.ANNO_STRIMZI_IO_DELETE_POD_AND_PVC, false))
                            .findFirst()
                            .orElse(null);

                    if (podToClean == null) {
                        // No pod is annotated for deletion => return success
                        return Future.succeededFuture();
                    } else {
                        return manualPodCleaning(podToClean.getMetadata().getName(), desiredPvcs);
                    }
                });
    }

    /**
     * Cleans a Pod and its PVCs if the user marked them for cleanup (deletion). It will first identify the existing
     * PVCs used by given Pod and the desired PVCs which need to be created after the old PVCs are deleted. Once
     * they are identified, it will start the deletion by deleting the controller resource.
     *
     * @param podName           Name of the Pod which should be cleaned / deleted
     * @param desiredPvcs       The list of desired PVCs which should be created after the old Pod and PVCs are deleted
     *
     * @return                  Future indicating the result of the cleanup
     */
    private Future<Void> manualPodCleaning(String podName, List<PersistentVolumeClaim> desiredPvcs) {
        return pvcOperator.listAsync(reconciliation.namespace(), selector)
                .compose(existingPvcs -> {
                    // Find out which PVCs need to be deleted
                    List<PersistentVolumeClaim> deletePvcs;

                    if (existingPvcs != null) {
                        deletePvcs = existingPvcs
                                .stream()
                                .filter(pvc -> pvc.getMetadata().getName().endsWith(podName))
                                .collect(Collectors.toList());
                    } else {
                        deletePvcs = new ArrayList<>(0);
                    }

                    // Find out which PVCs need to be created
                    List<PersistentVolumeClaim> createPvcs = desiredPvcs
                            .stream()
                            .filter(pvc -> pvc.getMetadata().getName().endsWith(podName))
                            .collect(Collectors.toList());

                    if (useStrimziPodSets) {
                        return cleanPodPvcAndPodSet(controllerResourceName, podName, createPvcs, deletePvcs);
                    } else {
                        return cleanPodPvcAndStatefulSet(controllerResourceName, podName, createPvcs, deletePvcs);
                    }
                });
    }

    /**
     * Handles the modification of the StrimziPodSet controlling the pod which should be cleaned. In order
     * to clean the pod and its PVCs, we first need to remove the pod from the StrimziPodSet. Otherwise, the
     * StrimziPodSet will break the process by recreating the pods or PVCs. This method first modifies the StrimziPodSet
     * and then calls other method to delete the Pod, PVCs and create the new PVCs. Once this method completes, it
     * will update the StrimziPodSet again. The Pod will be then recreated by the StrimziPodSet and this method just
     * waits for it to become ready.
     *
     * The complete flow looks like this
     *     1. Remove the deleted pod from the PodSet
     *     2. Trigger the Pod and PVC deletion and recreation
     *     3. Recreate the original PodSet
     *     4. Wait for the Pod to be created and become ready
     *
     * @param podSetName    Name of the StrimziPodSet to which this pod belongs
     * @param podName       Name of the Pod which should be cleaned / deleted
     * @param desiredPvcs   The list of desired PVCs which should be created after the old Pod and PVCs are deleted
     * @param currentPvcs   The list of current PVCs which should be deleted
     *
     * @return              Future indicating the result of the cleanup
     */
    private Future<Void> cleanPodPvcAndPodSet(String podSetName, String podName, List<PersistentVolumeClaim> desiredPvcs, List<PersistentVolumeClaim> currentPvcs) {
        return strimziPodSetOperator.getAsync(reconciliation.namespace(), podSetName)
                .compose(podSet -> {
                    List<Map<String, Object>> desiredPods = podSet.getSpec().getPods().stream()
                            .filter(pod -> !podName.equals(PodSetUtils.mapToPod(pod).getMetadata().getName()))
                            .collect(Collectors.toList());

                    StrimziPodSet reducedPodSet = new StrimziPodSetBuilder(podSet)
                            .editSpec()
                            .withPods(desiredPods)
                            .endSpec()
                            .build();

                    return strimziPodSetOperator.reconcile(reconciliation, reconciliation.namespace(), podSetName, reducedPodSet)
                            .compose(ignore -> cleanPodAndPvc(podName, desiredPvcs, currentPvcs))
                            .compose(ignore -> {
                                // We recreate the StrimziPodSet in its old configuration => any further changes have to be done by rolling update
                                // These fields need to be cleared before recreating the StatefulSet
                                podSet.getMetadata().setResourceVersion(null);
                                podSet.getMetadata().setSelfLink(null);
                                podSet.getMetadata().setUid(null);
                                podSet.setStatus(null);

                                return strimziPodSetOperator.reconcile(reconciliation, reconciliation.namespace(), podSetName, podSet);
                            })
                            .compose(ignore -> podOperator.readiness(reconciliation, reconciliation.namespace(), podName, 1_000L, operationTimeoutMs))
                            .map((Void) null);
                });
    }

    /**
     * Handles the deletion and recreation of the StatefulSet controlling the pod which should be cleaned. In order
     * to clean the pod and its PVCs, we first need to delete the StatefulSet (non-cascading). Otherwise, the
     * StatefulSet will break the process by recreating the pods or PVCs. This method first deletes the StatefulSet
     * and then calls other method to delete the Pod, PVCs and create the new PVCs. Once this method completes, it
     * will recreate the StatefulSet again. The Pod will be then recreated by the StatefulSet and this method just
     * waits for it to become ready.
     *
     * The complete flow looks like this
     *     1. Delete the STS (non-cascading)
     *     2. Trigger the Pod and PVC deletion and recreation
     *     3. Recreate the STS
     *     4. Wait for the Pod to be created and become ready
     *
     * @param stsName       NAme of the StatefulSet to which this pod belongs
     * @param podName       Name of the Pod which should be cleaned / deleted
     * @param desiredPvcs   The list of desired PVCs which should be created after the old Pod and PVCs are deleted
     * @param currentPvcs   The list of current PVCs which should be deleted
     *
     * @return              Future indicating the result of the cleanup
     */
    private Future<Void> cleanPodPvcAndStatefulSet(String stsName, String podName, List<PersistentVolumeClaim> desiredPvcs, List<PersistentVolumeClaim> currentPvcs) {
        return stsOperator.getAsync(reconciliation.namespace(), stsName)
                .compose(sts -> stsOperator.deleteAsync(reconciliation, reconciliation.namespace(), stsName, false)
                        .compose(ignore -> cleanPodAndPvc(podName, desiredPvcs, currentPvcs))
                        .compose(ignore -> {
                            // We recreate the StatefulSet in its old configuration => any further changes have to be done by rolling update
                            // These fields need to be cleared before recreating the StatefulSet
                            sts.getMetadata().setResourceVersion(null);
                            sts.getMetadata().setSelfLink(null);
                            sts.getMetadata().setUid(null);
                            sts.setStatus(null);

                            return stsOperator.reconcile(reconciliation, reconciliation.namespace(), stsName, sts);
                        })
                        .compose(ignore -> podOperator.readiness(reconciliation, reconciliation.namespace(), podName, 1_000L, operationTimeoutMs))
                        .map((Void) null));
    }

    /**
     * This is an internal method which actually executes the deletion of the Pod and PVC. This is a non-trivial
     * since the PVC and the Pod are tightly coupled and one cannot be deleted without the other. It will first
     * trigger the Pod deletion. Once the Pod is deleted, it will delete the PVCs. Once they are deleted as well, it
     * will create the new PVCs. The Pod is not recreated here => that is done by the controller (e.g. StatefulSet).
     *
     * This method expects that the StatefulSet or any other controller are already deleted to not interfere with
     * the process.
     *
     * To address these, we:
     *     1. Delete the Pod
     *     2. Wait for the Pod to be actually deleted
     *     3. Delete the PVC
     *     4. Wait for the PVCs to be actually deleted
     *     5. Recreate the PVCs
     *
     * @param podName           Name of the pod which should be deleted
     * @param deletePvcs        The list of PVCs which should be deleted
     * @param createPvcs        The list of PVCs which should be recreated
     *
     * @return                  Future which completes when the Pod and PVC are deleted
     */
    private Future<Void> cleanPodAndPvc(String podName, List<PersistentVolumeClaim> createPvcs, List<PersistentVolumeClaim> deletePvcs) {
        // First we delete the Pod which should be cleaned
        return podOperator.deleteAsync(reconciliation, reconciliation.namespace(), podName, true)
                .compose(ignore -> {
                    // With the pod deleted, we can delete all the PVCs belonging to this pod
                    @SuppressWarnings({ "rawtypes" }) // Has to use Raw type because of the CompositeFuture
                    List<Future> deleteResults = new ArrayList<>(deletePvcs.size());

                    for (PersistentVolumeClaim pvc : deletePvcs)    {
                        String pvcName = pvc.getMetadata().getName();
                        LOGGER.debugCr(reconciliation, "Deleting PVC {} for Pod {} based on {} annotation", pvcName, podName, AbstractScalableNamespacedResourceOperator.ANNO_STRIMZI_IO_DELETE_POD_AND_PVC);
                        deleteResults.add(pvcOperator.deleteAsync(reconciliation, reconciliation.namespace(), pvcName, true));
                    }
                    return CompositeFuture.join(deleteResults);
                })
                .compose(ignore -> {
                    // Once everything was deleted, we can recreate the PVCs
                    // The Pod will be recreated later when the controller resource is recreated
                    @SuppressWarnings({ "rawtypes" }) // Has to use Raw type because of the CompositeFuture
                    List<Future> createResults = new ArrayList<>(createPvcs.size());

                    for (PersistentVolumeClaim pvc : createPvcs)    {
                        LOGGER.debugCr(reconciliation, "Reconciling PVC {} for Pod {} after it was deleted and maybe recreated by the pod", pvc.getMetadata().getName(), podName);
                        createResults.add(pvcOperator.reconcile(reconciliation, reconciliation.namespace(), pvc.getMetadata().getName(), pvc));
                    }

                    return CompositeFuture.join(createResults);
                })
                .map((Void) null);
    }
}