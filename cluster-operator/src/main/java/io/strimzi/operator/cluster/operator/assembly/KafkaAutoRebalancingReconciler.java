/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.rebalance.KafkaAutoRebalanceModeBrokers;
import io.strimzi.api.kafka.model.rebalance.KafkaAutoRebalanceState;
import io.strimzi.api.kafka.model.rebalance.KafkaAutoRebalanceStatus;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceList;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceMode;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.CrdOperator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.vertx.core.Future;

import java.util.HashSet;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * This class runs the reconciliation for the auto-rebalancing process when the Kafka cluster is scaled up/down.
 */
public class KafkaAutoRebalancingReconciler {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaAutoRebalancingReconciler.class.getName());

    private final Reconciliation reconciliation;
    private Kafka kafkaCr;
    private final CrdOperator<KubernetesClient, KafkaRebalance, KafkaRebalanceList> kafkaRebalanceOperator;
    private final Set<Integer> toBeRemovedNodes;
    private final Set<Integer> addedNodes;

    /**
     * Constructs the Kafka auto-rebalancing reconciler
     *
     * @param reconciliation    Reconciliation marker
     * @param kafkaCr   The Kafka custom resource
     * @param supplier  Supplies the operators for different resources
     * @param toBeRemovedNodes  nodes to consider as being removed because of a scaling down but after the auto-rebalancing
     * @param addedNodes    nodes added because of a scaling up and to consider for the auto-rebalancing
     */
    public KafkaAutoRebalancingReconciler(
            Reconciliation reconciliation,
            Kafka kafkaCr,
            ResourceOperatorSupplier supplier,
            Set<Integer> toBeRemovedNodes,
            Set<Integer> addedNodes) {
        this.reconciliation = reconciliation;
        this.kafkaCr = kafkaCr;
        this.kafkaRebalanceOperator = supplier.kafkaRebalanceOperator;
        this.toBeRemovedNodes = toBeRemovedNodes;
        this.addedNodes = addedNodes;
    }

    /**
     * The main reconciliation method which triggers the whole reconciliation pipeline. This is the method which is
     * expected to be called from the outside to trigger the reconciliation.
     *
     * @param kafkaStatus The Kafka Status class for updating auto-rebalancing status on it during the reconciliation
     *
     * @return  Future which completes when the reconciliation completes
     */
    public Future<Void> reconcile(KafkaStatus kafkaStatus) {
        KafkaAutoRebalanceStatus kafkaAutoRebalanceStatus = kafkaStatus.getAutoRebalance();
        KafkaAutoRebalanceState autoRebalanceState = kafkaAutoRebalanceStatus != null ? kafkaAutoRebalanceStatus.getState() : KafkaAutoRebalanceState.Idle;
        LOGGER.infoCr(reconciliation, "Loaded auto-rebalance state from the Kafka CR [{}]", autoRebalanceState);

        if (kafkaAutoRebalanceStatus != null) {
            for (KafkaAutoRebalanceModeBrokers modeBrokers: kafkaAutoRebalanceStatus.getModes()) {
                switch (modeBrokers.getMode()) {
                    case REMOVE_BROKERS:
                        LOGGER.infoCr(reconciliation, "toBeRemovedNodes = {}", toBeRemovedNodes);
                        // TODO: TBD
                        // if empty -> no further action and stay with the current Kafka.status.autoRebalance.modes[remove-brokers].brokers list
                        // if not empty -> update the Kafka.status.autoRebalance.modes[remove-brokers].brokers by using the full content
                        //                 from the toBeRemovedNodes list which always contains the nodes involved in a scale down operation
                        if (!toBeRemovedNodes.isEmpty()) {
                            Set<Integer> newToBeRemovedNodes = toBeRemovedNodes;
                            LOGGER.infoCr(reconciliation, "newToBeRemovedNodes = {}", newToBeRemovedNodes);
                        }
                        break;
                    case ADD_BROKERS:
                        LOGGER.infoCr(reconciliation, "addedNodes = {}", addedNodes);
                        // TODO: TBD
                        // if empty -> no further action and stay with the current Kafka.status.autoRebalance.modes[add-brokers].brokers list
                        // if not empty -> update the Kafka.status.autoRebalance.modes[add-brokers].brokers by producing a consistent list
                        //                 with its current content and what is in the addedNodes list
                        if (!addedNodes.isEmpty()) {
                            Set<Integer> newAddedNodes = new HashSet<>(modeBrokers.getBrokers());
                            newAddedNodes.addAll(addedNodes);
                            LOGGER.infoCr(reconciliation, "newAddedNodes = {}", newAddedNodes);
                        }
                        break;
                    default:
                        return Future.failedFuture(new RuntimeException("Unexpected mode " + modeBrokers.getMode()));
                }
            }
        }

        switch (autoRebalanceState) {
            case Idle:
                return onIdle();
            case RebalanceOnScaleDown:
                return onRebalanceOnScaleDown(kafkaAutoRebalanceStatus);
            case RebalanceOnScaleUp:
                return onRebalanceOnScaleUp(kafkaAutoRebalanceStatus);
            default:
                return Future.failedFuture(new RuntimeException("Unexpected state " + autoRebalanceState));
        }
    }

    private Future<Void> onIdle() {
        // TODO:
        // if there is a queued rebalancing scale down (Kafka.status.autoRebalance.modes[remove-brokers] exists), start the rebalancing
        // scale down and transition to RebalanceOnScaleDown.
        if (!toBeRemovedNodes.isEmpty()) {
            // TODO: TBD -> create KafkaRebalance for auto-rebalancing scaling down, move to RebalanceOnScaleDown
        // TODO:
        // If no queued rebalancing scale down but there is a queued rebalancing scale up (Kafka.status.autoRebalance.modes[add-brokers] exists),
        // start the rebalancing scale up and transition to RebalanceOnScaleUp.
        } else if (!addedNodes.isEmpty()) {
            // TODO: TBD -> create KafkaRebalance for auto-rebalancing scaling up, move to RebalanceOnScaleUp
        }
        // TODO: TBD
        // No queued rebalancing (so no scale down/up requested), stay in Idle.
        return Future.succeededFuture();
    }

    private Future<Void> onRebalanceOnScaleDown(KafkaAutoRebalanceStatus kafkaAutoRebalanceStatus) {
        this.getKafkaRebalance(kafkaCr.getMetadata().getNamespace(), kafkaCr.getMetadata().getName(), KafkaRebalanceMode.REMOVE_BROKERS)
                .compose(kafkaRebalance -> {

                    handleKafkaRebalance(kafkaRebalance, kafkaAutoRebalanceStatus,
                            (removeBrokersMode, addBrokersMode) -> {
                                // TODO:
                                // check if Kafka.status.autoRebalance.modes[remove-brokers].brokers was updated compared to the current running rebalancing scale down.
                                // If different, start the rebalancing and stay in RebalanceOnScaleDown.
                                // If no changes, if there is a queued rebalancing scale up (Kafka.status.autoRebalance.modes[add-brokers] exists), start the
                                // rebalancing and transition to RebalanceOnScaleUp, or just transition to Idle if there is not, clean
                                // Kafka.status.autoRebalance.modes and delete the "actual" KafkaRebalance custom resource.
                                LOGGER.infoCr(reconciliation, "onRebalanceOnScaleDown rebalanceReady");
                                return KafkaAutoRebalanceState.Idle;
                            },
                            (removeBrokersMode, addBrokersMode) -> {
                                // TODO:
                                // check if Kafka.status.autoRebalance.modes[remove-brokers].brokers was updated compared to the current running
                                // rebalancing scale down. If different, update the corresponding KafkaRebalance in order to take into account the updated
                                // brokers list and refresh it by applying the strimzi.io/rebalance: refresh annotation. Stay in RebalanceOnScaleDown.
                                // If no changes, no further action and stay in RebalanceOnScaleDown.
                                LOGGER.infoCr(reconciliation, "onRebalanceOnScaleDown rebalanceRunning");
                                return KafkaAutoRebalanceState.Idle;
                            },
                            (removeBrokersMode, addBrokersMode) -> {
                                // TODO:
                                // the rebalancing scale down failed, transition to Idle and also removing the corresponding mode and brokers list from the
                                // status. The operator also deletes the "actual" KafkaRebalance custom resource.
                                LOGGER.infoCr(reconciliation, "onRebalanceOnScaleDown rebalanceNotReady");
                                return KafkaAutoRebalanceState.Idle;
                            });

                    // TODO: TBD
                    return null;

                }, exception -> Future.failedFuture(exception));

        // TODO: TBD
        return Future.succeededFuture();
    }

    private Future<Void> onRebalanceOnScaleUp(KafkaAutoRebalanceStatus kafkaAutoRebalanceStatus) {
        this.getKafkaRebalance(kafkaCr.getMetadata().getNamespace(), kafkaCr.getMetadata().getName(), KafkaRebalanceMode.ADD_BROKERS)
                .compose(kafkaRebalance -> {

                    handleKafkaRebalance(kafkaRebalance, kafkaAutoRebalanceStatus,
                            (removeBrokersMode, addBrokersMode) -> {
                                // TODO:
                                // if there is a queued rebalancing scale down (Kafka.status.autoRebalance.modes[remove-brokers] exists), start the
                                // rebalancing scale down and transition to RebalanceOnScaleDown.
                                // If no queued rebalancing scale down, check if Kafka.status.autoRebalance.modes[add-brokers].brokers was updated
                                // compared to the current running rebalancing scale up. If no changes, no further actions but just transition to Idle, clean
                                // Kafka.status.autoRebalance.modes and delete the "actual" KafkaRebalance custom resource. If different, update the
                                // corresponding KafkaRebalance in order to take into account the updated brokers list and refresh it by applying the strimzi.io/rebalance: refresh annotation. Stay in RebalanceOnScaleUp.
                                LOGGER.infoCr(reconciliation, "onRebalanceOnScaleUp rebalanceReady");
                                return KafkaAutoRebalanceState.Idle;
                            },
                            (removeBrokersMode, addBrokersMode) -> {
                                // TODO:
                                // if there is a queued rebalancing scale down (Kafka.status.autoRebalance.modes[remove-brokers] exists), stop the current
                                // rebalancing scale up by applying the strimzi.io/rebalance: stop annotation on the corresponding KafkaRebalance. Start
                                // the rebalancing scale down and transition to RebalanceOnScaleDown.
                                // If no queued rebalancing scale down, check if Kafka.status.autoRebalance.modes[add-brokers].brokers was updated
                                // compared to the current running rebalancing scale up. If no changes, no further actions. If different, update the
                                // corresponding KafkaRebalance in order to take into account the updated brokers list and refresh it by applying the
                                // strimzi.io/rebalance: refresh annotation. Stay in RebalanceOnScaleUp.
                                LOGGER.infoCr(reconciliation, "onRebalanceOnScaleUp rebalanceRunning");
                                return KafkaAutoRebalanceState.Idle;
                            },
                            (removeBrokersMode, addBrokersMode) -> {
                                // TODO:
                                // the rebalancing scale up failed, transition to Idle and also removing the corresponding mode and brokers list from the status.
                                // The operator also deletes the "actual" KafkaRebalance custom resource.
                                LOGGER.infoCr(reconciliation, "onRebalanceOnScaleUp rebalanceNotReady");
                                return KafkaAutoRebalanceState.Idle;
                            });

                    // TODO: TBD
                    return null;

                }, exception -> Future.failedFuture(exception));

        // TODO: TBD
        return Future.succeededFuture();
    }

    private void handleKafkaRebalance(
            KafkaRebalance kafkaRebalance,
            KafkaAutoRebalanceStatus kafkaAutoRebalanceStatus,
            BiFunction<KafkaAutoRebalanceModeBrokers, KafkaAutoRebalanceModeBrokers, KafkaAutoRebalanceState> rebalanceReady,
            BiFunction<KafkaAutoRebalanceModeBrokers, KafkaAutoRebalanceModeBrokers, KafkaAutoRebalanceState> rebalanceRunning,
            BiFunction<KafkaAutoRebalanceModeBrokers, KafkaAutoRebalanceModeBrokers, KafkaAutoRebalanceState> rebalanceNotReady) {

        KafkaRebalanceState kafkaRebalanceState = null; // TODO: get the actual state from the kafkaRebalance

        KafkaAutoRebalanceModeBrokers removeBrokersMode = null;
        KafkaAutoRebalanceModeBrokers addBrokersMode = null;

        for (KafkaAutoRebalanceModeBrokers mode: kafkaAutoRebalanceStatus.getModes()) {
            switch (mode.getMode()) {
                case REMOVE_BROKERS -> removeBrokersMode = mode;
                case ADD_BROKERS -> addBrokersMode = mode;
            }
        }

        switch (kafkaRebalanceState) {
            case Ready ->
                    rebalanceReady.apply(removeBrokersMode, addBrokersMode);
            case PendingProposal, ProposalReady, Rebalancing ->
                    rebalanceRunning.apply(removeBrokersMode, addBrokersMode);
            case NotReady ->
                    rebalanceNotReady.apply(removeBrokersMode, addBrokersMode);
        }
    }

    private Future<KafkaRebalance> getKafkaRebalance(String namespace, String cluster, KafkaRebalanceMode kafkaRebalanceMode) {
        return kafkaRebalanceOperator.getAsync(namespace, cluster + "-auto-rebalancing-" + kafkaRebalanceMode);
    }
}
