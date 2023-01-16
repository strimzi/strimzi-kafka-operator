/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.strimzi.operator.cluster.model.StorageUtils;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.operator.resource.PvcOperator;
import io.strimzi.operator.common.operator.resource.StorageClassOperator;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;

/**
 * This class reconciles the PVCs for the Kafka and ZooKeeper clusters. It has two public methods:
 *   - resizeAndReconcilePvcs for creating, updating and resizing PVCs which are needed by the cluster
 *   - deletePersistentClaims method for deleting PVCs not needed anymore and marked for deletion
 */
public class PvcReconciler {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(PvcReconciler.class.getName());

    private final Reconciliation reconciliation;
    private final PvcOperator pvcOperator;
    private final StorageClassOperator storageClassOperator;

    /**
     * Constructs the PVC Reconciler
     *
     * @param reconciliation        Reconciliation marker
     * @param pvcOperator           The Persistent Volume Claim operator for working with Kubernetes PVC
     * @param storageClassOperator  The Storage Class operator for working with Kubernetes Storage Classes
     */
    public PvcReconciler(Reconciliation reconciliation, PvcOperator pvcOperator, StorageClassOperator storageClassOperator) {
        this.reconciliation = reconciliation;
        this.pvcOperator = pvcOperator;
        this.storageClassOperator = storageClassOperator;
    }

    /**
     * Resizes and reconciles the PVCs based on the model and list of PVCs passed to it. It will return a future with
     * collection containing a list of pods which need restart to complete the filesystem resizing. The PVCs are only
     * created or updated. This method does not delete any PVCs. This is done by a separate method which should be
     * called separately at the end of the reconciliation.
     *
     * @param podNameProvider   Function to generate a pod name from its index
     * @param pvcs              List of desired PVC used by this controller
     *
     * @return                  Future with list of pod names which should be restarted to complete the filesystem resizing
     */
    public Future<Collection<String>> resizeAndReconcilePvcs(Function<Integer, String> podNameProvider, List<PersistentVolumeClaim> pvcs) {
        Set<String> podsToRestart = new HashSet<>();
        @SuppressWarnings({ "rawtypes" }) // Has to use Raw type because of the CompositeFuture
        List<Future> futures = new ArrayList<>(pvcs.size());

        for (PersistentVolumeClaim desiredPvc : pvcs)  {
            Future<Void> perPvcFuture = pvcOperator.getAsync(reconciliation.namespace(), desiredPvc.getMetadata().getName())
                    .compose(currentPvc -> {
                        if (currentPvc == null || currentPvc.getStatus() == null || !"Bound".equals(currentPvc.getStatus().getPhase())) {
                            // This branch handles the following conditions:
                            // * The PVC doesn't exist yet, we should create it
                            // * The PVC is not Bound, we should reconcile it
                            return pvcOperator.reconcile(reconciliation, reconciliation.namespace(), desiredPvc.getMetadata().getName(), desiredPvc)
                                    .map((Void) null);
                        } else if (currentPvc.getStatus().getConditions().stream().anyMatch(cond -> "Resizing".equals(cond.getType()) && "true".equals(cond.getStatus().toLowerCase(Locale.ENGLISH))))  {
                            // The PVC is Bound, but it is already resizing => Nothing to do, we should let it resize
                            LOGGER.debugCr(reconciliation, "The PVC {} is resizing, nothing to do", desiredPvc.getMetadata().getName());
                            return Future.succeededFuture();
                        } else if (currentPvc.getStatus().getConditions().stream().anyMatch(cond -> "FileSystemResizePending".equals(cond.getType()) && "true".equals(cond.getStatus().toLowerCase(Locale.ENGLISH))))  {
                            // The PVC is Bound and resized but waiting for FS resizing => We need to restart the pod which is using it
                            String podName = podNameProvider.apply(getPodIndexFromPvcName(desiredPvc.getMetadata().getName()));
                            podsToRestart.add(podName);
                            LOGGER.infoCr(reconciliation, "The PVC {} is waiting for file system resizing and the pod {} needs to be restarted.", desiredPvc.getMetadata().getName(), podName);
                            return Future.succeededFuture();
                        } else {
                            // The PVC is Bound and resizing is not in progress => We should check if the SC supports resizing and check if size changed
                            Long currentSize = StorageUtils.convertToMillibytes(currentPvc.getSpec().getResources().getRequests().get("storage"));
                            Long desiredSize = StorageUtils.convertToMillibytes(desiredPvc.getSpec().getResources().getRequests().get("storage"));

                            if (!currentSize.equals(desiredSize))   {
                                // The sizes are different => we should resize (shrinking will be handled in StorageDiff, so we do not need to check that)
                                return resizePvc(currentPvc, desiredPvc);
                            } else  {
                                // size didn't change, just reconcile
                                return pvcOperator.reconcile(reconciliation, reconciliation.namespace(), desiredPvc.getMetadata().getName(), desiredPvc)
                                        .map((Void) null);
                            }
                        }
                    });

            futures.add(perPvcFuture);
        }

        return CompositeFuture.all(futures)
                .map(podsToRestart);
    }

    /**
     * Resizes a PVC. This includes the check whether the Storage Class used by this PVC allows volume resizing. This
     * method does not wait for the resizing to happen. It just requests it from Kubernetes / Storage Class.
     *
     * @param current   The current PVC with the old size
     * @param desired   The desired PVC with the new size
     *
     * @return          Future which completes when the PVC / PV resizing is completed.
     */
    private Future<Void> resizePvc(PersistentVolumeClaim current, PersistentVolumeClaim desired)  {
        String storageClassName = current.getSpec().getStorageClassName();

        if (storageClassName != null && !storageClassName.isEmpty()) {
            return storageClassOperator.getAsync(storageClassName)
                    .compose(sc -> {
                        if (sc == null) {
                            LOGGER.warnCr(reconciliation, "Storage Class {} not found. PVC {} cannot be resized. Reconciliation will proceed without reconciling this PVC.", storageClassName, desired.getMetadata().getName());
                            return Future.succeededFuture();
                        } else if (sc.getAllowVolumeExpansion() == null || !sc.getAllowVolumeExpansion())    {
                            // Resizing not supported in SC => do nothing
                            LOGGER.warnCr(reconciliation, "Storage Class {} does not support resizing of volumes. PVC {} cannot be resized. Reconciliation will proceed without reconciling this PVC.", storageClassName, desired.getMetadata().getName());
                            return Future.succeededFuture();
                        } else  {
                            // Resizing supported by SC => We can reconcile the PVC to have it resized
                            LOGGER.infoCr(reconciliation, "Resizing PVC {} from {} to {}.", desired.getMetadata().getName(), current.getStatus().getCapacity().get("storage").getAmount(), desired.getSpec().getResources().getRequests().get("storage").getAmount());
                            return pvcOperator.reconcile(reconciliation, reconciliation.namespace(), desired.getMetadata().getName(), desired)
                                    .map((Void) null);
                        }
                    });
        } else {
            LOGGER.warnCr(reconciliation, "PVC {} does not use any Storage Class and cannot be resized. Reconciliation will proceed without reconciling this PVC.", desired.getMetadata().getName());
            return Future.succeededFuture();
        }
    }

    /**
     * Deletes the PCVs which are not needed anymore and which have the deleteClaim flag set to true.
     *
     * @param maybeDeletePvcs   List of existing PVCs which should be considered for deletion
     * @param desiredPvcs       List of PVCs which should be kept
     *
     * @return                  Future which completes when all PVCs which needed to be deleted were deleted
     */
    public Future<Void> deletePersistentClaims(List<String> maybeDeletePvcs, List<String> desiredPvcs) {
        @SuppressWarnings({ "rawtypes" }) // Has to use Raw type because of the CompositeFuture
        List<Future> futures = new ArrayList<>();

        maybeDeletePvcs.removeAll(desiredPvcs);

        for (String pvcName : maybeDeletePvcs)  {
            LOGGER.debugCr(reconciliation, "Considering PVC {} for deletion", pvcName);
            futures.add(considerPersistentClaimDeletion(pvcName));
        }

        return CompositeFuture.all(futures)
                .map((Void) null);
    }

    /**
     * Gets the PVC resource and checks if it has the delete-claim annotation set to true. If it does, it deletes it.
     *
     * @param pvcName   Name of the PVC to consider for deletion
     *
     * @return          Future which completes when the PVC is deleted or when we find out that it should not be deleted
     */
    private Future<Void> considerPersistentClaimDeletion(String pvcName)   {
        return pvcOperator.getAsync(reconciliation.namespace(), pvcName)
                .compose(pvc -> {
                    if (Annotations.booleanAnnotation(pvc, Annotations.ANNO_STRIMZI_IO_DELETE_CLAIM, false)) {
                        LOGGER.infoCr(reconciliation, "Deleting PVC {}", pvcName);
                        return pvcOperator.reconcile(reconciliation, reconciliation.namespace(), pvcName, null)
                                .map((Void) null);
                    } else {
                        return Future.succeededFuture();
                    }
                });
    }

    /**
     * Internal method used to detect pod sequence ID from the name of the PVC
     *
     * @param pvcName   Name of the PVC claim for which the Pod index should be detected
     *
     * @return          The index of the pod to which this PVC belongs
     */
    private static int getPodIndexFromPvcName(String pvcName)  {
        return Integer.parseInt(pvcName.substring(pvcName.lastIndexOf("-") + 1));
    }
}