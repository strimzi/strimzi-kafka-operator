/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.vertx.core.Future;

import java.util.Set;

/**
 * This class runs the reconciliation for the auto-rebalancing process when the Kafka cluster is scaled up/down.
 */
public class KafkaAutoRebalancingReconciler {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaAutoRebalancingReconciler.class.getName());

    private final Reconciliation reconciliation;
    private final Set<Integer> toBeRemovedNodes;
    private final Set<Integer> addedNodes;

    /**
     * Constructs the Kafka auto-rebalancing reconciler
     *
     * @param reconciliation    Reconciliation marker
     * @param toBeRemovedNodes  nodes to consider as being removed because of a scaling down but after the auto-rebalancing
     * @param addedNodes    nodes added because of a scaling up and to consider for the auto-rebalancing
     */
    public KafkaAutoRebalancingReconciler(
            Reconciliation reconciliation,
            Set<Integer> toBeRemovedNodes,
            Set<Integer> addedNodes) {
        this.reconciliation = reconciliation;
        this.toBeRemovedNodes = toBeRemovedNodes;
        this.addedNodes = addedNodes;
    }

    /**
     * The main reconciliation method which triggers the whole reconciliation pipeline. This is the method which is
     * expected to be called from the outside to trigger the reconciliation.
     *
     * @return  Future which completes when the reconciliation completes
     */
    public Future<Void> reconcile() {
        // TODO: TBD
        return Future.succeededFuture();
    }
}
