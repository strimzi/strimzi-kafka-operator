/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.PvcOperator;
import io.strimzi.operator.common.operator.resource.StorageClassOperator;
import io.vertx.core.Future;

import java.util.List;

/**
 * Class used for method relevant for reconciliation of Strimzi objects.
 */
public abstract class AbstractReconciler {

    protected final Reconciliation reconciliation;
    protected final PvcOperator pvcOperator;
    protected final StorageClassOperator storageClassOperator;

    protected AbstractReconciler(Reconciliation reconciliation, PvcOperator pvcOperator, StorageClassOperator storageClassOperator) {
        this.reconciliation = reconciliation;
        this.pvcOperator = pvcOperator;
        this.storageClassOperator = storageClassOperator;
    }

    /**
     * Deletion of PVCs after the cluster is deleted is handled by owner reference and garbage collection. However,
     * this would not help after scale-downs. Therefore, we check if there are any PVCs which should not be present
     * and delete them when they are.
     * This should be called only after the StrimziPodSet reconciliation, rolling update and scale-down when the PVCs
     * are not used any more by the pods.
     *
     * @param expectedPvcs   List of PVCs which should be present
     * @param selectorLabels Labels used for selecting the PVCs
     * @return  Future which completes when the PVCs which should be deleted are deleted
     */
    protected Future<Void> deletePersistentClaims(List<String> expectedPvcs, Labels selectorLabels) {
        return pvcOperator.listAsync(reconciliation.namespace(), selectorLabels)
                .compose(pvcs -> {
                    List<String> maybeDeletePvcs = pvcs.stream().map(pvc -> pvc.getMetadata().getName()).toList();
                    return new PvcReconciler(reconciliation, pvcOperator, storageClassOperator)
                            .deletePersistentClaims(maybeDeletePvcs, expectedPvcs);
                });
    }
}
