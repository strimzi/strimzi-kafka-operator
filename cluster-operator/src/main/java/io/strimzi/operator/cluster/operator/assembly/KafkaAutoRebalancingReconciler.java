/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.rebalance.KafkaAutoRebalanceConfiguration;
import io.strimzi.api.kafka.model.rebalance.KafkaAutoRebalanceModeBrokers;
import io.strimzi.api.kafka.model.rebalance.KafkaAutoRebalanceModeBrokersBuilder;
import io.strimzi.api.kafka.model.rebalance.KafkaAutoRebalanceState;
import io.strimzi.api.kafka.model.rebalance.KafkaAutoRebalanceStatus;
import io.strimzi.api.kafka.model.rebalance.KafkaAutoRebalanceStatusBuilder;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceAnnotation;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceBuilder;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceList;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceMode;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.CrdOperator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.StatusUtils;
import io.vertx.core.Future;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.strimzi.api.ResourceAnnotations.ANNO_STRIMZI_IO_REBALANCE;
import static io.strimzi.api.ResourceAnnotations.ANNO_STRIMZI_IO_REBALANCE_AUTOAPPROVAL;

/**
 * This class runs the reconciliation for the auto-rebalancing process when the Kafka cluster is scaled up/down.
 */
public class KafkaAutoRebalancingReconciler {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaAutoRebalancingReconciler.class.getName());

    private static final String STRIMZI_IO_AUTO_REBALANCING_FINALIZER = "strimzi.io/auto-rebalancing";

    private final Reconciliation reconciliation;
    private final Kafka kafkaCr;
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
        // Cruise Control is not defined, so nothing to reconcile
        if (kafkaCr.getSpec().getCruiseControl() == null) {
            LOGGER.infoCr(reconciliation, "Cruise Control not defined in the Kafka custom resource, no auto-rebalancing to reconcile");
            return Future.succeededFuture();
        }

        KafkaAutoRebalanceStatus kafkaAutoRebalanceStatus = kafkaCr.getStatus() != null ? kafkaCr.getStatus().getAutoRebalance() : null;

        if (kafkaAutoRebalanceStatus != null) {
            LOGGER.infoCr(reconciliation, "Loaded auto-rebalance state from the Kafka CR [{}]", kafkaAutoRebalanceStatus.getState());
            return computeNextStatus(kafkaAutoRebalanceStatus)
                    .onComplete(v -> kafkaStatus.setAutoRebalance(kafkaAutoRebalanceStatus));
        } else {
            LOGGER.infoCr(reconciliation, "No auto-rebalance state from the Kafka CR, initializing to [Idle]");
            // when the auto-rebalance status doesn't exist, the Kafka cluster is being created
            // so auto-rebalance can be set in an Idle state, no further actions
            kafkaStatus.setAutoRebalance(
                    new KafkaAutoRebalanceStatusBuilder()
                            .withState(KafkaAutoRebalanceState.Idle)
                            .withLastTransitionTime(StatusUtils.iso8601Now())
                            .build());
            return Future.succeededFuture();
        }
    }

    private Future<Void> computeNextStatus(KafkaAutoRebalanceStatus kafkaAutoRebalanceStatus) {
        Nodes nodes = getNodesToBeRemovedAdded(kafkaAutoRebalanceStatus);
        switch (kafkaAutoRebalanceStatus.getState()) {
            case Idle:
                return onIdle(kafkaAutoRebalanceStatus, nodes);
            case RebalanceOnScaleDown:
                return onRebalanceOnScaleDown(kafkaAutoRebalanceStatus, nodes);
            case RebalanceOnScaleUp:
                return onRebalanceOnScaleUp(kafkaAutoRebalanceStatus, nodes);
            default:
                return Future.failedFuture(new RuntimeException("Unexpected state " + kafkaAutoRebalanceStatus.getState()));
        }
    }

    private Future<Void> onIdle(KafkaAutoRebalanceStatus kafkaAutoRebalanceStatus, Nodes nodes) {
        if (!nodes.toBeRemoved().isEmpty()) {
            // TODO:
            // if there is a queued rebalancing scale down (Kafka.status.autoRebalance.modes[remove-brokers] exists), start the rebalancing
            // scale down and transition to RebalanceOnScaleDown.
            // TODO: TBD -> create KafkaRebalance for auto-rebalancing scaling down, move to RebalanceOnScaleDown
            return createKafkaRebalance(kafkaCr.getMetadata().getNamespace(), kafkaCr.getMetadata().getName(), KafkaRebalanceMode.REMOVE_BROKERS, nodes.toBeRemoved().stream().toList())
                    .compose(v -> {
                        updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.RebalanceOnScaleDown, nodes);
                        return Future.succeededFuture();
                    });
        } else if (!nodes.added().isEmpty()) {
            // TODO:
            // If no queued rebalancing scale down but there is a queued rebalancing scale up (Kafka.status.autoRebalance.modes[add-brokers] exists),
            // start the rebalancing scale up and transition to RebalanceOnScaleUp.
            // TODO: TBD -> create KafkaRebalance for auto-rebalancing scaling up, move to RebalanceOnScaleUp
            return createKafkaRebalance(kafkaCr.getMetadata().getNamespace(), kafkaCr.getMetadata().getName(), KafkaRebalanceMode.ADD_BROKERS, nodes.added().stream().toList())
                    .compose(v -> {
                        updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.RebalanceOnScaleUp, nodes);
                        return Future.succeededFuture();
                    });
        }
        // TODO:
        // No queued rebalancing (so no scale down/up requested), stay in Idle.
        updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.Idle, new Nodes(Set.of(), Set.of()));
        return Future.succeededFuture();
    }

    private Future<Void> onRebalanceOnScaleDown(KafkaAutoRebalanceStatus kafkaAutoRebalanceStatus, Nodes nodes) {
        return getKafkaRebalance(kafkaCr.getMetadata().getNamespace(), kafkaCr.getMetadata().getName(), KafkaRebalanceMode.REMOVE_BROKERS)
                .compose(kafkaRebalance -> {

                    KafkaRebalanceState kafkaRebalanceState = KafkaRebalanceUtils.rebalanceState(kafkaRebalance.getStatus());
                    switch (kafkaRebalanceState) {
                        case Ready:
                            // TODO:
                            // check if Kafka.status.autoRebalance.modes[remove-brokers].brokers was updated compared to the current running rebalancing scale down.
                            // If different, start the rebalancing and stay in RebalanceOnScaleDown.
                            // If no changes, if there is a queued rebalancing scale up (Kafka.status.autoRebalance.modes[add-brokers] exists), start the
                            // rebalancing and transition to RebalanceOnScaleUp, or just transition to Idle if there is not, clean
                            // Kafka.status.autoRebalance.modes and delete the "actual" KafkaRebalance custom resource.

                            // check if Kafka.status.autoRebalance.modes[remove-brokers].brokers was updated compared to the current running rebalancing scale down

                            // If different ...
                            if (!nodes.toBeRemoved().equals(kafkaRebalance.getSpec().getBrokers().stream().collect(Collectors.toSet()))) {
                                // start the rebalancing and stay in RebalanceOnScaleDown
                                refreshKafkaRebalance(kafkaRebalance, nodes.toBeRemoved().stream().toList());
                                updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.RebalanceOnScaleDown, nodes);
                            } else {
                                // If no changes ...
                                if (nodes.added().isEmpty()) {
                                    // no queued rebalancing scale up (Kafka.status.autoRebalance.modes[add-brokers] not exists) just transition to Idle, clean
                                    // Kafka.status.autoRebalance.modes and delete the "actual" KafkaRebalance custom resource
                                    deleteKafkaRebalance(kafkaRebalance);
                                    updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.Idle, new Nodes(Set.of(), Set.of()));
                                } else {
                                    // if there is a queued rebalancing scale up (Kafka.status.autoRebalance.modes[add-brokers] exists), start the
                                    // rebalancing and transition to RebalanceOnScaleUp
                                    return createKafkaRebalance(kafkaCr.getMetadata().getNamespace(), kafkaCr.getMetadata().getName(), KafkaRebalanceMode.ADD_BROKERS, nodes.added().stream().toList())
                                            .compose(v -> {
                                                updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.RebalanceOnScaleUp, nodes);
                                                return Future.succeededFuture();
                                            });
                                }
                            }

                            LOGGER.infoCr(reconciliation, "onRebalanceOnScaleDown rebalanceReady");
                            break;
                        case New:
                        case PendingProposal:
                        case ProposalReady:
                        case Rebalancing:
                            // TODO:
                            // check if Kafka.status.autoRebalance.modes[remove-brokers].brokers was updated compared to the current running
                            // rebalancing scale down. If different, update the corresponding KafkaRebalance in order to take into account the updated
                            // brokers list and refresh it by applying the strimzi.io/rebalance: refresh annotation. Stay in RebalanceOnScaleDown.
                            // If no changes, no further action and stay in RebalanceOnScaleDown.

                            // If different ...
                            if (!nodes.toBeRemoved().equals(kafkaRebalance.getSpec().getBrokers().stream().collect(Collectors.toSet()))) {
                                // update the corresponding KafkaRebalance in order to take into account the updated
                                // brokers list and refresh it by applying the strimzi.io/rebalance: refresh annotation. Stay in RebalanceOnScaleDown.
                                refreshKafkaRebalance(kafkaRebalance, nodes.toBeRemoved().stream().toList());
                                updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.RebalanceOnScaleDown, nodes);
                            } else {
                                // If no changes ... no further action and stay in RebalanceOnScaleDown.
                                // TODO: to be verified that nodes are not changed so the status is the previous one
                                updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.RebalanceOnScaleDown, nodes);
                            }

                            LOGGER.infoCr(reconciliation, "onRebalanceOnScaleDown rebalanceRunning");
                            break;
                        case NotReady:
                            // TODO:
                            // the rebalancing scale down failed, transition to Idle and also removing the corresponding mode and brokers list from the
                            // status. The operator also deletes the "actual" KafkaRebalance custom resource.

                            deleteKafkaRebalance(kafkaRebalance);
                            updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.Idle, new Nodes(Set.of(), Set.of()));

                            LOGGER.infoCr(reconciliation, "onRebalanceOnScaleDown rebalanceNotReady");
                            break;
                    }
                    return Future.succeededFuture();
                }, exception -> Future.failedFuture(exception));
    }

    private Future<Void> onRebalanceOnScaleUp(KafkaAutoRebalanceStatus kafkaAutoRebalanceStatus, Nodes nodes) {
        return getKafkaRebalance(kafkaCr.getMetadata().getNamespace(), kafkaCr.getMetadata().getName(), KafkaRebalanceMode.ADD_BROKERS)
                .compose(kafkaRebalance -> {

                    KafkaRebalanceState kafkaRebalanceState = KafkaRebalanceUtils.rebalanceState(kafkaRebalance.getStatus());
                    switch (kafkaRebalanceState) {
                        case Ready:
                            // TODO:
                            // if there is a queued rebalancing scale down (Kafka.status.autoRebalance.modes[remove-brokers] exists), start the
                            // rebalancing scale down and transition to RebalanceOnScaleDown.
                            // If no queued rebalancing scale down, check if Kafka.status.autoRebalance.modes[add-brokers].brokers was updated
                            // compared to the current running rebalancing scale up. If no changes, no further actions but just transition to Idle, clean
                            // Kafka.status.autoRebalance.modes and delete the "actual" KafkaRebalance custom resource. If different, update the
                            // corresponding KafkaRebalance in order to take into account the updated brokers list and refresh it by applying the strimzi.io/rebalance: refresh annotation. Stay in RebalanceOnScaleUp.

                            if (!nodes.toBeRemoved().isEmpty()) {
                                // if there is a queued rebalancing scale down (Kafka.status.autoRebalance.modes[remove-brokers] exists), start the
                                // rebalancing scale down and transition to RebalanceOnScaleDown.
                                deleteKafkaRebalance(kafkaRebalance);
                                return createKafkaRebalance(kafkaCr.getMetadata().getNamespace(), kafkaCr.getMetadata().getName(), KafkaRebalanceMode.REMOVE_BROKERS, nodes.toBeRemoved().stream().toList())
                                        .compose(v -> {
                                            updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.RebalanceOnScaleDown, nodes);
                                            return Future.succeededFuture();
                                        });
                            } else {
                                // If no queued rebalancing scale down, check if Kafka.status.autoRebalance.modes[add-brokers].brokers was updated
                                // compared to the current running rebalancing scale up.
                                if (!nodes.added().equals(kafkaRebalance.getSpec().getBrokers().stream().collect(Collectors.toSet()))) {
                                    // If different, update the corresponding KafkaRebalance in order to take into account the updated brokers list
                                    // and refresh it by applying the strimzi.io/rebalance: refresh annotation. Stay in RebalanceOnScaleUp.
                                    refreshKafkaRebalance(kafkaRebalance, nodes.added().stream().toList());
                                    updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.RebalanceOnScaleUp, nodes);
                                } else {
                                    // If no changes, no further actions but just transition to Idle, clean
                                    // Kafka.status.autoRebalance.modes and delete the "actual" KafkaRebalance custom resource.
                                    deleteKafkaRebalance(kafkaRebalance);
                                    updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.Idle, new Nodes(Set.of(), Set.of()));
                                }
                            }

                            LOGGER.infoCr(reconciliation, "onRebalanceOnScaleUp rebalanceReady");
                            break;
                        case New:
                        case PendingProposal:
                        case ProposalReady:
                        case Rebalancing:
                            // TODO:
                            // if there is a queued rebalancing scale down (Kafka.status.autoRebalance.modes[remove-brokers] exists), stop the current
                            // rebalancing scale up by applying the strimzi.io/rebalance: stop annotation on the corresponding KafkaRebalance. Start
                            // the rebalancing scale down and transition to RebalanceOnScaleDown.
                            // If no queued rebalancing scale down, check if Kafka.status.autoRebalance.modes[add-brokers].brokers was updated
                            // compared to the current running rebalancing scale up. If no changes, no further actions. If different, update the
                            // corresponding KafkaRebalance in order to take into account the updated brokers list and refresh it by applying the
                            // strimzi.io/rebalance: refresh annotation. Stay in RebalanceOnScaleUp.

                            if (!nodes.toBeRemoved().isEmpty()) {
                                // if there is a queued rebalancing scale down (Kafka.status.autoRebalance.modes[remove-brokers] exists), stop the current
                                // rebalancing scale up by applying the strimzi.io/rebalance: stop annotation on the corresponding KafkaRebalance. Start
                                // the rebalancing scale down and transition to RebalanceOnScaleDown.

                                // TODO: evaluate race condition between stop and delete (does CC stopped rebalance before we delete)
                                stopKafkaRebalance(kafkaRebalance);
                                deleteKafkaRebalance(kafkaRebalance);

                                return createKafkaRebalance(kafkaCr.getMetadata().getNamespace(), kafkaCr.getMetadata().getName(), KafkaRebalanceMode.REMOVE_BROKERS, nodes.toBeRemoved().stream().toList())
                                        .compose(v -> {
                                            updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.RebalanceOnScaleDown, nodes);
                                            return Future.succeededFuture();
                                        });
                            } else {
                                // If no queued rebalancing scale down, check if Kafka.status.autoRebalance.modes[add-brokers].brokers was updated
                                // compared to the current running rebalancing scale up
                                if (!nodes.added().equals(kafkaRebalance.getSpec().getBrokers().stream().collect(Collectors.toSet()))) {
                                    // If different, update the corresponding KafkaRebalance in order to take into account the updated brokers list
                                    // and refresh it by applying the strimzi.io/rebalance: refresh annotation. Stay in RebalanceOnScaleUp.
                                    refreshKafkaRebalance(kafkaRebalance, nodes.added().stream().toList());
                                    updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.RebalanceOnScaleUp, nodes);
                                } else {
                                    // If no changes ... no further action and stay in RebalanceOnScaleUp.
                                    // TODO: to be verified that nodes are not changed so the status is the previous one
                                    updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.RebalanceOnScaleUp, nodes);
                                }
                            }

                            LOGGER.infoCr(reconciliation, "onRebalanceOnScaleUp rebalanceRunning");
                            break;
                        case NotReady:
                            // TODO:
                            // the rebalancing scale up failed, transition to Idle and also removing the corresponding mode and brokers list from the status.
                            // The operator also deletes the "actual" KafkaRebalance custom resource.

                            deleteKafkaRebalance(kafkaRebalance);
                            kafkaAutoRebalanceStatus.setState(KafkaAutoRebalanceState.Idle);
                            kafkaAutoRebalanceStatus.setLastTransitionTime(StatusUtils.iso8601Now());
                            kafkaAutoRebalanceStatus.setModes(null);

                            LOGGER.infoCr(reconciliation, "onRebalanceOnScaleUp rebalanceNotReady");
                            break;
                    }
                    return Future.succeededFuture();
                }, exception -> Future.failedFuture(exception));
    }

    private boolean isToBeRemovedNodes() {
        return toBeRemovedNodes != null && !toBeRemovedNodes.isEmpty();
    }

    private boolean isAddedNodes() {
        return addedNodes != null && !addedNodes.isEmpty();
    }

    private Nodes getNodesToBeRemovedAdded(final KafkaAutoRebalanceStatus kafkaAutoRebalanceStatus) {
        Set<Integer> newToBeRemovedNodes = new HashSet<>();
        Set<Integer> newAddedNodes = new HashSet<>();

        LOGGER.infoCr(reconciliation, "toBeRemovedNodes = {}", toBeRemovedNodes);
        LOGGER.infoCr(reconciliation, "addedNodes = {}", addedNodes);

        if (kafkaAutoRebalanceStatus.getModes() == null) {
            if (isToBeRemovedNodes()) {
                newToBeRemovedNodes.addAll(toBeRemovedNodes);
            }
            if (isAddedNodes()) {
                newAddedNodes.addAll(addedNodes);
            }
        } else {
            for (KafkaAutoRebalanceModeBrokers modeBrokers : kafkaAutoRebalanceStatus.getModes()) {
                switch (modeBrokers.getMode()) {
                    case REMOVE_BROKERS:
                        // TODO: TBD
                        // if not empty -> update the Kafka.status.autoRebalance.modes[remove-brokers].brokers by using the full content
                        //                 from the toBeRemovedNodes list which always contains the nodes involved in a scale down operation
                        // if empty -> no further action and stay with the current Kafka.status.autoRebalance.modes[remove-brokers].brokers list
                        if (isToBeRemovedNodes()) {
                            newToBeRemovedNodes.addAll(toBeRemovedNodes);
                        } else {
                            newToBeRemovedNodes.addAll(modeBrokers.getBrokers());
                        }
                        break;
                    case ADD_BROKERS:
                        // TODO: TBD
                        // if not empty -> update the Kafka.status.autoRebalance.modes[add-brokers].brokers by producing a consistent list
                        //                 with its current content and what is in the addedNodes list
                        // if empty -> no further action and stay with the current Kafka.status.autoRebalance.modes[add-brokers].brokers list
                        if (isAddedNodes()) {
                            newAddedNodes.addAll(addedNodes);
                        }
                        newAddedNodes.addAll(modeBrokers.getBrokers());
                        break;
                    default:
                        throw new RuntimeException("Unexpected mode " + modeBrokers.getMode());
                }
            }
        }
        LOGGER.infoCr(reconciliation, "newToBeRemovedNodes = {}", newToBeRemovedNodes);
        LOGGER.infoCr(reconciliation, "newAddedNodes = {}", newAddedNodes);
        return new Nodes(newToBeRemovedNodes, newAddedNodes);
    }

    private Future<KafkaRebalance> getKafkaRebalance(String namespace, String cluster, KafkaRebalanceMode kafkaRebalanceMode) {
        return kafkaRebalanceOperator.getAsync(namespace, KafkaRebalanceUtils.autoRebalancingKafkaRebalanceResourceName(cluster, kafkaRebalanceMode));
    }

    private Future<Void> createKafkaRebalance(String namespace, String cluster, KafkaRebalanceMode kafkaRebalanceMode, List<Integer> brokers) {
        Optional<KafkaAutoRebalanceConfiguration> config =
                kafkaCr.getSpec().getCruiseControl().getAutoRebalance().stream().filter(c -> c.getMode().equals(kafkaRebalanceMode)).findFirst();
        if (config.isEmpty()) {
            return Future.failedFuture(new RuntimeException("No configuration specified for mode " + kafkaRebalanceMode));
        }

        if (config.get().getTemplate() != null) {
            return kafkaRebalanceOperator.getAsync(namespace, config.get().getTemplate().getName())
                    .compose(kafkaRebalanceTemplate -> {
                        KafkaRebalance kafkaRebalance = new KafkaRebalanceBuilder()
                                .withNewMetadata()
                                    .withName(KafkaRebalanceUtils.autoRebalancingKafkaRebalanceResourceName(cluster, kafkaRebalanceMode))
                                    .addToAnnotations(ANNO_STRIMZI_IO_REBALANCE_AUTOAPPROVAL, "true")
                                    .addToFinalizers(STRIMZI_IO_AUTO_REBALANCING_FINALIZER)
                                .endMetadata()
                                .withSpec(kafkaRebalanceTemplate.getSpec())
                                .editSpec()
                                    .withMode(kafkaRebalanceMode)
                                    .withBrokers(brokers)
                                .endSpec()
                                .build();
                        kafkaRebalanceOperator.client().inNamespace(kafkaCr.getMetadata().getNamespace()).resource(kafkaRebalance).create();
                        return Future.succeededFuture();
                    });
        } else {
            return Future.succeededFuture();
        }
    }

    private void deleteKafkaRebalance(KafkaRebalance kafkaRebalance) {
        // remove the finalizer to allow the deletion
        kafkaRebalanceOperator.client().inNamespace(kafkaRebalance.getMetadata().getNamespace()).resource(kafkaRebalance).edit(
                kr -> new KafkaRebalanceBuilder(kr)
                        .editMetadata()
                            .removeFromFinalizers(STRIMZI_IO_AUTO_REBALANCING_FINALIZER)
                        .endMetadata()
                        .build()
        );
        kafkaRebalanceOperator.client().inNamespace(kafkaRebalance.getMetadata().getNamespace()).resource(kafkaRebalance).delete();
    }

    private void refreshKafkaRebalance(KafkaRebalance kafkaRebalance, List<Integer> brokers) {
        kafkaRebalanceOperator.client().inNamespace(kafkaRebalance.getMetadata().getNamespace()).resource(kafkaRebalance).edit(
                kr -> new KafkaRebalanceBuilder(kr)
                        .editMetadata()
                            .addToAnnotations(Map.of(ANNO_STRIMZI_IO_REBALANCE, KafkaRebalanceAnnotation.refresh.toString()))
                        .endMetadata()
                        .editSpec()
                            .withBrokers(brokers)
                        .endSpec()
                        .build()
        );
    }

    private void stopKafkaRebalance(KafkaRebalance kafkaRebalance) {
        kafkaRebalanceOperator.client().inNamespace(kafkaRebalance.getMetadata().getNamespace()).resource(kafkaRebalance).edit(
                kr -> new KafkaRebalanceBuilder(kr)
                        .editMetadata()
                            .addToAnnotations(Map.of(ANNO_STRIMZI_IO_REBALANCE, KafkaRebalanceAnnotation.stop.toString()))
                        .endMetadata()
                        .build()
        );
    }

    private void updateStatus(KafkaAutoRebalanceStatus kafkaAutoRebalanceStatus, KafkaAutoRebalanceState state, Nodes nodes) {
        // just clear the modes field when there are no added nodes or to be removed
        List<KafkaAutoRebalanceModeBrokers> modes = null;
        if (!nodes.toBeRemoved().isEmpty() || !nodes.added().isEmpty()) {
            modes = new ArrayList<>(2);
            if (!nodes.toBeRemoved().isEmpty()) {
                modes.add(
                        new KafkaAutoRebalanceModeBrokersBuilder()
                                .withMode(KafkaRebalanceMode.REMOVE_BROKERS)
                                .withBrokers(nodes.toBeRemoved().stream().toList())
                                .build()
                );
            }
            if (!nodes.added().isEmpty()) {
                modes.add(
                        new KafkaAutoRebalanceModeBrokersBuilder()
                                .withMode(KafkaRebalanceMode.ADD_BROKERS)
                                .withBrokers(nodes.added().stream().toList())
                                .build()
                );
            }
        }
        kafkaAutoRebalanceStatus.setState(state);
        kafkaAutoRebalanceStatus.setLastTransitionTime(StatusUtils.iso8601Now());
        kafkaAutoRebalanceStatus.setModes(modes);
    }

    /**
     * Utility class to take the updated to be removed and added nodes
     */
    record Nodes(Set<Integer> toBeRemoved, Set<Integer> added) { }
}
