/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.api.kafka.model.podset.StrimziPodSetBuilder;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PvcOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StrimziPodSetOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.Labels;
import io.vertx.core.Future;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This class takes care of manually cleaning pods belonging to a specific controller if it is requested by the user
 * using an annotation. Pod clean-up in this case means deletion of the pod and related PVCs. The Pod and PVCs will be
 * recreated only by the further parts of the KafkaAssemblyOperator.
 */
public class ManualPodCleaner {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ManualPodCleaner.class.getName());

    private final Reconciliation reconciliation;
    private final Labels selector;

    private final PodOperator podOperator;
    private final PvcOperator pvcOperator;
    private final StrimziPodSetOperator strimziPodSetOperator;

    /**
     * Constructs the Manual Pod Cleaner.
     *
     * @param reconciliation    Reconciliation marker
     * @param selector          Selector for selecting the Pods belonging to this controller
     * @param supplier          Resource Operator Supplier with the Kubernetes resource operators
     */
    public ManualPodCleaner(
            Reconciliation reconciliation,
            Labels selector,
            ResourceOperatorSupplier supplier
    ) {
        this.reconciliation = reconciliation;
        this.selector = selector;

        this.strimziPodSetOperator = supplier.strimziPodSetOperator;
        this.pvcOperator = supplier.pvcOperations;
        this.podOperator = supplier.podOperations;
    }

    /**
     * Constructs the Manual Pod Cleaner
     *
     * @param reconciliation            Reconciliation marker
     * @param selector                  Selector for selecting the Pods belonging to this controller
     * @param strimziPodSetOperator     The Pod Set operator for working with Strimzi Pod Sets
     * @param podOperator               The Pod operator for working with Kubernetes Pods
     * @param pvcOperator               The Persistent Volume Claim operator for working with Kubernetes PVC
     */
    public ManualPodCleaner(
            Reconciliation reconciliation,
            Labels selector,

            StrimziPodSetOperator strimziPodSetOperator,
            PodOperator podOperator,
            PvcOperator pvcOperator
    ) {
        this.reconciliation = reconciliation;
        this.selector = selector;

        this.strimziPodSetOperator = strimziPodSetOperator;
        this.pvcOperator = pvcOperator;
        this.podOperator = podOperator;
    }

    /**
     * Checks pods to see whether the user requested them to be deleted including their PVCs. If the user requested it,
     * it will delete them. In a single reconciliation, always only one Pod is deleted. If multiple pods are marked for
     * cleanup, they will be done in subsequent reconciliations. This method only checks if cleanup was requested and
     * calls other methods to execute it. The Pod and PVCs will be recreated only by the further parts of the
     * KafkaAssemblyOperator.
     *
     * @return  Future indicating the result of the cleanup operation. Returns always success if there are no pods to clean.
     */
    public Future<Void> maybeManualPodCleaning() {
        return podOperator.listAsync(reconciliation.namespace(), selector)
                .compose(pods -> {
                    // Only one pod per reconciliation is rolled
                    Pod podToClean = pods
                            .stream()
                            .filter(pod -> Annotations.booleanAnnotation(pod, Annotations.ANNO_STRIMZI_IO_DELETE_POD_AND_PVC, false))
                            .findFirst()
                            .orElse(null);

                    if (podToClean == null) {
                        // No pod is annotated for deletion => return success
                        return Future.succeededFuture();
                    } else {
                        return manualPodCleaning(podToClean.getMetadata().getName());
                    }
                });
    }

    /**
     * Cleans a Pod and its PVCs if the user marked them for cleanup (deletion). It will first identify the existing
     * PVCs used by given Pod and the desired PVCs which need to be created after the old PVCs are deleted. Once
     * they are identified, it will start the deletion by removing it from the PodSet and then deleting the actual Pod
     * and PVC.
     *
     * @param podName           Name of the Pod which should be cleaned / deleted
     *
     * @return                  Future indicating the result of the cleanup
     */
    private Future<Void> manualPodCleaning(String podName) {
        return pvcOperator.listAsync(reconciliation.namespace(), selector)
                .compose(existingPvcs -> {
                    // Find out which PVCs need to be deleted
                    List<PersistentVolumeClaim> deletePvcs;

                    if (existingPvcs != null) {
                        deletePvcs = existingPvcs
                                .stream()
                                .filter(pvc -> pvc.getMetadata().getName().endsWith(podName))
                                .toList();
                    } else {
                        deletePvcs = List.of();
                    }

                    return cleanPodPvcAndPodSet(ReconcilerUtils.getControllerNameFromPodName(podName), podName, deletePvcs);
                });
    }

    /**
     * Handles the modification of the StrimziPodSet controlling the pod which should be cleaned. In order
     * to clean the pod and its PVCs, we first need to remove the pod from the StrimziPodSet. Otherwise, the
     * StrimziPodSet will break the process by recreating the pods or PVCs. This method first modifies the StrimziPodSet
     * and then calls other method to delete the Pod and PVCs.
     *
     * The complete flow looks like this
     *     1. Remove the deleted pod from the PodSet
     *     2. Trigger the Pod and PVC deletion
     *
     * @param podSetName    Name of the StrimziPodSet to which this pod belongs
     * @param podName       Name of the Pod which should be cleaned / deleted
     * @param deletePvcs   The list of current PVCs which should be deleted
     *
     * @return              Future indicating the result of the cleanup
     */
    private Future<Void> cleanPodPvcAndPodSet(String podSetName, String podName, List<PersistentVolumeClaim> deletePvcs) {
        return strimziPodSetOperator.getAsync(reconciliation.namespace(), podSetName)
                .compose(podSet -> {
                    List<Map<String, Object>> desiredPods = podSet.getSpec().getPods().stream()
                            .filter(pod -> !podName.equals(PodSetUtils.mapToPod(pod).getMetadata().getName()))
                            .toList();

                    // New PodSet without the Pod we are going to delete
                    StrimziPodSet reducedPodSet = new StrimziPodSetBuilder(podSet)
                            .editSpec()
                            .withPods(desiredPods)
                            .endSpec()
                            .build();

                    return strimziPodSetOperator.reconcile(reconciliation, reconciliation.namespace(), podSetName, reducedPodSet)
                            .compose(ignore -> deletePodAndPvc(podName, deletePvcs))
                            .map((Void) null);
                });
    }

    /**
     * This is an internal method which actually executes the deletion of the Pod and PVC. This is a non-trivial
     * since the PVC and the Pod are tightly coupled and one cannot be deleted without the other. It will first
     * trigger the Pod deletion. Once the Pod is deleted, it will delete the PVCs.
     *
     * This method expects that the StrimziPodSet or any other controller are already deleted (or modified by removing
     * the pod from it) to not interfere with the process.
     *
     * To address these, we:
     *     1. Delete the Pod
     *     2. Wait for the Pod to be actually deleted
     *     3. Delete the PVC
     *     4. Wait for the PVCs to be actually deleted
     *
     * @param podName           Name of the pod which should be deleted
     * @param deletePvcs        The list of PVCs which should be deleted
     *
     * @return                  Future which completes when the Pod and PVC are deleted
     */
    private Future<Void> deletePodAndPvc(String podName, List<PersistentVolumeClaim> deletePvcs) {
        // First we delete the Pod which should be cleaned
        return podOperator.deleteAsync(reconciliation, reconciliation.namespace(), podName, true)
                .compose(ignore -> {
                    // With the pod deleted, we can delete all the PVCs belonging to this pod
                    List<Future<Void>> deleteResults = new ArrayList<>(deletePvcs.size());

                    for (PersistentVolumeClaim pvc : deletePvcs)    {
                        String pvcName = pvc.getMetadata().getName();
                        LOGGER.debugCr(reconciliation, "Deleting PVC {} for Pod {} based on {} annotation", pvcName, podName, Annotations.ANNO_STRIMZI_IO_DELETE_POD_AND_PVC);
                        deleteResults.add(pvcOperator.deleteAsync(reconciliation, reconciliation.namespace(), pvcName, true));
                    }
                    return Future.join(deleteResults);
                })
                .map((Void) null);
    }
}