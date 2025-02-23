/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceConfiguration;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceMode;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceState;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceStatus;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceStatusBrokers;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceStatusBrokersBuilder;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceStatusBuilder;
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
import io.strimzi.operator.common.model.Labels;
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

    private static final ScalingNodes EMPTY_SCALING_NODES = new ScalingNodes(Set.of(), Set.of());

    private final Reconciliation reconciliation;
    private final KafkaAutoRebalanceStatus kafkaAutoRebalanceStatus;
    private final List<KafkaAutoRebalanceConfiguration> kafkaAutoRebalanceConfigurations;
    private final CrdOperator<KubernetesClient, KafkaRebalance, KafkaRebalanceList> kafkaRebalanceOperator;
    private final Set<Integer> scalingDownBlockedNodes;

    /**
     * Constructs the Kafka auto-rebalancing reconciler
     *
     * @param reconciliation    Reconciliation marker
     * @param kafkaCr   The Kafka custom resource
     * @param supplier  Supplies the operators for different resources
     * @param scalingDownBlockedNodes  nodes blocked on scaling down because of the need for auto-rebalancing first
     */
    public KafkaAutoRebalancingReconciler(
            Reconciliation reconciliation,
            Kafka kafkaCr,
            ResourceOperatorSupplier supplier,
            Set<Integer> scalingDownBlockedNodes) {
        this.reconciliation = reconciliation;
        // load current autorebalance status if it exists (before starting the reconciliation) or initialize it to Idle
        this.kafkaAutoRebalanceStatus =
                (kafkaCr.getStatus() != null && kafkaCr.getStatus().getAutoRebalance() != null) ?
                        kafkaCr.getStatus().getAutoRebalance() :
                        new KafkaAutoRebalanceStatusBuilder()
                                .withState(KafkaAutoRebalanceState.Idle)
                                .withLastTransitionTime(StatusUtils.iso8601Now())
                                .build();
        this.kafkaAutoRebalanceConfigurations = kafkaCr.getSpec().getCruiseControl().getAutoRebalance();
        this.kafkaRebalanceOperator = supplier.kafkaRebalanceOperator;
        this.scalingDownBlockedNodes = scalingDownBlockedNodes;
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
        ScalingNodes scalingNodes = getScalingNodes(kafkaStatus.getAutoRebalance());
        if (!scalingNodes.isEmpty()) {
            LOGGER.infoCr(reconciliation, "Reconciling auto-rebalance in the [{}] state with scaling nodes: blocked scale down = {}, added scale up = {}",
                    kafkaAutoRebalanceStatus.getState(), scalingNodes.blocked(), scalingNodes.added());
        }
        return maybeRebalance(scalingNodes)
                .onComplete(v -> kafkaStatus.setAutoRebalance(kafkaAutoRebalanceStatus));
    }

    private Future<Void> maybeRebalance(ScalingNodes scalingNodes) {
        return switch (kafkaAutoRebalanceStatus.getState()) {
            case Idle -> onIdle(scalingNodes);
            case RebalanceOnScaleDown -> onRebalanceOnScaleDown(scalingNodes);
            case RebalanceOnScaleUp -> onRebalanceOnScaleUp(scalingNodes);
        };
    }

    private Future<Void> onIdle(ScalingNodes scalingNodes) {
        if (!scalingNodes.blocked().isEmpty()) {
            // if there is a queued rebalancing scale down (Kafka.status.autoRebalance.modes[remove-brokers] exists), start the rebalancing
            // scale down and transition to RebalanceOnScaleDown.
            return createKafkaRebalance(reconciliation.namespace(), reconciliation.name(), KafkaAutoRebalanceMode.REMOVE_BROKERS, scalingNodes.blocked().stream().toList())
                    .compose(created -> {
                        if (created) {
                            updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.RebalanceOnScaleDown, scalingNodes);
                        } else {
                            updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.Idle, EMPTY_SCALING_NODES);
                        }
                        return Future.succeededFuture();
                    });
        } else if (!scalingNodes.added().isEmpty()) {
            // If no queued rebalancing scale down but there is a queued rebalancing scale up (Kafka.status.autoRebalance.modes[add-brokers] exists),
            // start the rebalancing scale up and transition to RebalanceOnScaleUp.
            return createKafkaRebalance(reconciliation.namespace(), reconciliation.name(), KafkaAutoRebalanceMode.ADD_BROKERS, scalingNodes.added().stream().toList())
                    .compose(created -> {
                        if (created) {
                            updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.RebalanceOnScaleUp, scalingNodes);
                        } else {
                            updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.Idle, EMPTY_SCALING_NODES);
                        }
                        return Future.succeededFuture();
                    });
        }
        // No queued rebalancing (so no scale down/up requested), stay in Idle, no status update
        return Future.succeededFuture();
    }

    private Future<Void> onRebalanceOnScaleDown(ScalingNodes scalingNodes) {
        return getKafkaRebalance(reconciliation.namespace(), reconciliation.name(), KafkaAutoRebalanceMode.REMOVE_BROKERS)
                .compose(kafkaRebalance -> {

                    KafkaRebalanceState kafkaRebalanceState = KafkaRebalanceUtils.rebalanceState(kafkaRebalance.getStatus());
                    LOGGER.infoCr(reconciliation, "Auto-rebalance on scaling down with KafkaRebalance {}/{} in state [{}]",
                            kafkaRebalance.getMetadata().getNamespace(),
                            kafkaRebalance.getMetadata().getName(),
                            kafkaRebalanceState);
                    switch (kafkaRebalanceState) {
                        case Ready:
                            // check if Kafka.status.autoRebalance.modes[remove-brokers].brokers was updated compared to the current running rebalancing scale down.
                            // If different, start the rebalancing and stay in RebalanceOnScaleDown.
                            // If no changes, if there is a queued rebalancing scale up (Kafka.status.autoRebalance.modes[add-brokers] exists), start the
                            // rebalancing and transition to RebalanceOnScaleUp, or just transition to Idle if there is not, clean
                            // Kafka.status.autoRebalance.modes and delete the "actual" KafkaRebalance custom resource.

                            // check if Kafka.status.autoRebalance.modes[remove-brokers].brokers was updated compared to the current running rebalancing scale down

                            // If different ...
                            if (!scalingNodes.blocked().isEmpty() &&
                                    !scalingNodes.blocked().equals(kafkaRebalance.getSpec().getBrokers().stream().collect(Collectors.toSet()))) {
                                // start the rebalancing and stay in RebalanceOnScaleDown
                                return refreshKafkaRebalance(kafkaRebalance, scalingNodes.blocked().stream().toList())
                                        .compose(v -> {
                                            updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.RebalanceOnScaleDown, scalingNodes);
                                            return Future.succeededFuture();
                                        });
                            } else {
                                // deleting the resource related to the current rebalancing on scale down just completed
                                return deleteKafkaRebalance(kafkaRebalance)
                                        .compose(v -> {
                                            // If no changes ...
                                            if (scalingNodes.added().isEmpty()) {
                                                // no queued rebalancing scale up (Kafka.status.autoRebalance.modes[add-brokers] not exists) just transition to Idle, clean
                                                // Kafka.status.autoRebalance.modes and delete the "actual" KafkaRebalance custom resource
                                                updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.Idle, EMPTY_SCALING_NODES);
                                                return Future.succeededFuture();
                                            } else {
                                                // if there is a queued rebalancing scale up (Kafka.status.autoRebalance.modes[add-brokers] exists), start the
                                                // rebalancing and transition to RebalanceOnScaleUp
                                                return createKafkaRebalance(reconciliation.namespace(), reconciliation.name(), KafkaAutoRebalanceMode.ADD_BROKERS, scalingNodes.added().stream().toList())
                                                        .compose(created -> {
                                                            if (created) {
                                                                updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.RebalanceOnScaleUp, scalingNodes);
                                                            } else {
                                                                updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.Idle, EMPTY_SCALING_NODES);
                                                            }
                                                            return Future.succeededFuture();
                                                        });
                                            }
                                        });
                            }
                        case New:
                        case PendingProposal:
                        case ProposalReady:
                        case Rebalancing:
                            // check if Kafka.status.autoRebalance.modes[remove-brokers].brokers was updated compared to the current running
                            // rebalancing scale down. If different, update the corresponding KafkaRebalance in order to take into account the updated
                            // brokers list and refresh it by applying the strimzi.io/rebalance: refresh annotation. Stay in RebalanceOnScaleDown.
                            // If no changes, no further action and stay in RebalanceOnScaleDown.

                            // If different ...
                            if (!scalingNodes.blocked().isEmpty() &&
                                    !scalingNodes.blocked().equals(kafkaRebalance.getSpec().getBrokers().stream().collect(Collectors.toSet()))) {
                                // update the corresponding KafkaRebalance in order to take into account the updated
                                // brokers list and refresh it by applying the strimzi.io/rebalance: refresh annotation. Stay in RebalanceOnScaleDown.
                                return refreshKafkaRebalance(kafkaRebalance, scalingNodes.blocked().stream().toList())
                                        .compose(v -> {
                                            updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.RebalanceOnScaleDown, scalingNodes);
                                            return Future.succeededFuture();
                                        });
                            } else {
                                // If no changes ... no further action and stay in RebalanceOnScaleDown.
                                return Future.succeededFuture();
                            }
                        case NotReady:
                            // the rebalancing scale down failed, transition to Idle and also removing the corresponding mode and brokers list from the
                            // status. The operator also deletes the "actual" KafkaRebalance custom resource.
                            return deleteKafkaRebalance(kafkaRebalance)
                                    .compose(v -> {
                                        updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.Idle, EMPTY_SCALING_NODES);
                                        return Future.succeededFuture();
                                    });
                        default:
                            return Future.failedFuture(new RuntimeException("Unexpected state " + kafkaRebalanceState));
                    }
                });
    }

    private Future<Void> onRebalanceOnScaleUp(ScalingNodes scalingNodes) {
        return getKafkaRebalance(reconciliation.namespace(), reconciliation.name(), KafkaAutoRebalanceMode.ADD_BROKERS)
                .compose(kafkaRebalance -> {

                    KafkaRebalanceState kafkaRebalanceState = KafkaRebalanceUtils.rebalanceState(kafkaRebalance.getStatus());
                    LOGGER.infoCr(reconciliation, "Auto-rebalance on scaling up with KafkaRebalance {}/{} in state [{}]",
                            kafkaRebalance.getMetadata().getNamespace(),
                            kafkaRebalance.getMetadata().getName(),
                            kafkaRebalanceState);
                    switch (kafkaRebalanceState) {
                        case Ready:
                            // if there is a queued rebalancing scale down (Kafka.status.autoRebalance.modes[remove-brokers] exists), start the
                            // rebalancing scale down and transition to RebalanceOnScaleDown.
                            // If no queued rebalancing scale down, check if Kafka.status.autoRebalance.modes[add-brokers].brokers was updated
                            // compared to the current running rebalancing scale up. If no changes, no further actions but just transition to Idle, clean
                            // Kafka.status.autoRebalance.modes and delete the "actual" KafkaRebalance custom resource. If different, update the
                            // corresponding KafkaRebalance in order to take into account the updated brokers list and refresh it by applying the strimzi.io/rebalance: refresh annotation. Stay in RebalanceOnScaleUp.

                            if (!scalingNodes.blocked().isEmpty()) {
                                // if there is a queued rebalancing scale down (Kafka.status.autoRebalance.modes[remove-brokers] exists), start the
                                // rebalancing scale down and transition to RebalanceOnScaleDown.
                                return deleteKafkaRebalance(kafkaRebalance)
                                        .compose(v -> createKafkaRebalance(reconciliation.namespace(), reconciliation.name(), KafkaAutoRebalanceMode.REMOVE_BROKERS, scalingNodes.blocked().stream().toList()))
                                        .compose(created -> {
                                            if (created) {
                                                updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.RebalanceOnScaleDown, scalingNodes);
                                            } else {
                                                updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.Idle, EMPTY_SCALING_NODES);
                                            }
                                            return Future.succeededFuture();
                                        });
                            } else {
                                // If no queued rebalancing scale down, check if Kafka.status.autoRebalance.modes[add-brokers].brokers was updated
                                // compared to the current running rebalancing scale up.
                                if (!scalingNodes.added().equals(kafkaRebalance.getSpec().getBrokers().stream().collect(Collectors.toSet()))) {
                                    // If different, update the corresponding KafkaRebalance in order to take into account the updated brokers list
                                    // and refresh it by applying the strimzi.io/rebalance: refresh annotation. Stay in RebalanceOnScaleUp.
                                    return refreshKafkaRebalance(kafkaRebalance, scalingNodes.added().stream().toList())
                                            .compose(v -> {
                                                updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.RebalanceOnScaleUp, scalingNodes);
                                                return Future.succeededFuture();
                                            });
                                } else {
                                    // If no changes, no further actions but just transition to Idle, clean
                                    // Kafka.status.autoRebalance.modes and delete the "actual" KafkaRebalance custom resource.
                                    return deleteKafkaRebalance(kafkaRebalance)
                                            .compose(v -> {
                                                updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.Idle, EMPTY_SCALING_NODES);
                                                return Future.succeededFuture();
                                            });
                                }
                            }
                        case New:
                        case PendingProposal:
                        case ProposalReady:
                        case Rebalancing:
                            // if there is a queued rebalancing scale down (Kafka.status.autoRebalance.modes[remove-brokers] exists), stop the current
                            // rebalancing scale up by applying the strimzi.io/rebalance: stop annotation on the corresponding KafkaRebalance. Start
                            // the rebalancing scale down and transition to RebalanceOnScaleDown.
                            // If no queued rebalancing scale down, check if Kafka.status.autoRebalance.modes[add-brokers].brokers was updated
                            // compared to the current running rebalancing scale up. If no changes, no further actions. If different, update the
                            // corresponding KafkaRebalance in order to take into account the updated brokers list and refresh it by applying the
                            // strimzi.io/rebalance: refresh annotation. Stay in RebalanceOnScaleUp.

                            if (!scalingNodes.blocked().isEmpty()) {
                                // if there is a queued rebalancing scale down (Kafka.status.autoRebalance.modes[remove-brokers] exists), stop the current
                                // rebalancing scale up by applying the strimzi.io/rebalance: stop annotation on the corresponding KafkaRebalance. Start
                                // the rebalancing scale down and transition to RebalanceOnScaleDown.
                                return stopKafkaRebalance(kafkaRebalance)
                                        // getting the patched version of the KafkaRebalance (stopped by annotation) for the next deletion operation (which patches again to remove finalizer)
                                        .compose(v -> getKafkaRebalance(reconciliation.namespace(), reconciliation.name(), KafkaAutoRebalanceMode.ADD_BROKERS))
                                        .compose(stoppedKafkaRebalance -> deleteKafkaRebalance(stoppedKafkaRebalance))
                                        .compose(v -> createKafkaRebalance(reconciliation.namespace(), reconciliation.name(), KafkaAutoRebalanceMode.REMOVE_BROKERS, scalingNodes.blocked().stream().toList()))
                                        .compose(created -> {
                                            if (created) {
                                                // if nodes blocked for scaling down are the same of the nodes that were added for scale up,
                                                // after scaling down them, we don't need to continue the auto-rebalancing for scale up anymore
                                                // (nodes are not there anymore!) so not queueing added nodes in the status
                                                updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.RebalanceOnScaleDown,
                                                        !scalingNodes.blocked().equals(scalingNodes.added()) ? scalingNodes :
                                                        new ScalingNodes(scalingNodes.blocked(), Set.of()));
                                            } else {
                                                updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.Idle, EMPTY_SCALING_NODES);
                                            }
                                            return Future.succeededFuture();
                                        });
                            } else {
                                // If no queued rebalancing scale down, check if Kafka.status.autoRebalance.modes[add-brokers].brokers was updated
                                // compared to the current running rebalancing scale up
                                if (!scalingNodes.added().equals(kafkaRebalance.getSpec().getBrokers().stream().collect(Collectors.toSet()))) {
                                    // If different, update the corresponding KafkaRebalance in order to take into account the updated brokers list
                                    // and refresh it by applying the strimzi.io/rebalance: refresh annotation. Stay in RebalanceOnScaleUp.
                                    return refreshKafkaRebalance(kafkaRebalance, scalingNodes.added().stream().toList())
                                            .compose(v -> {
                                                updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.RebalanceOnScaleUp, scalingNodes);
                                                return Future.succeededFuture();
                                            });
                                } else {
                                    // If no changes ... no further action and stay in RebalanceOnScaleUp.
                                    return Future.succeededFuture();
                                }
                            }
                        case NotReady:
                            // If NotReady but there is a new scale-up request, it could be Cruise Control not rolled yet and we can refresh to retry
                            if (!scalingNodes.added().equals(kafkaRebalance.getSpec().getBrokers().stream().collect(Collectors.toSet()))) {
                                // If different, update the corresponding KafkaRebalance in order to take into account the updated brokers list
                                // and refresh it by applying the strimzi.io/rebalance: refresh annotation. Stay in RebalanceOnScaleUp.
                                return refreshKafkaRebalance(kafkaRebalance, scalingNodes.added().stream().toList())
                                        .compose(v -> {
                                            updateStatus(kafkaAutoRebalanceStatus, KafkaAutoRebalanceState.RebalanceOnScaleUp, scalingNodes);
                                            return Future.succeededFuture();
                                        });
                            } else {
                                // the rebalancing scale up failed, transition to Idle and also removing the corresponding mode and brokers list from the status.
                                // The operator also deletes the "actual" KafkaRebalance custom resource.
                                return deleteKafkaRebalance(kafkaRebalance)
                                        .compose(v -> {
                                            kafkaAutoRebalanceStatus.setState(KafkaAutoRebalanceState.Idle);
                                            kafkaAutoRebalanceStatus.setLastTransitionTime(StatusUtils.iso8601Now());
                                            kafkaAutoRebalanceStatus.setModes(null);
                                            return Future.succeededFuture();
                                        });
                            }
                        default:
                            return Future.failedFuture(new RuntimeException("Unexpected state " + kafkaRebalanceState));
                    }
                });
    }

    private ScalingNodes getScalingNodes(final KafkaAutoRebalanceStatus kafkaAutoRebalanceStatus) {
        // if not empty -> update the Kafka.status.autoRebalance.modes[remove-brokers].brokers by using the full content
        //                 from the scalingDownBlockedNodes list which always contains the nodes involved in a scale down operation
        // if empty -> no further action and stay with the current Kafka.status.autoRebalance.modes[remove-brokers].brokers list

        // NOTHING TO DO on the scaling down block nodes, we always get the updated ones through the reconciliation, nothing to get from the status

        // if not empty -> update the Kafka.status.autoRebalance.modes[add-brokers].brokers by producing a consistent list
        //                 with its current content and what is in the scalingUpAddedNodes list
        // if empty -> no further action and stay with the current Kafka.status.autoRebalance.modes[add-brokers].brokers list

        // LOADING from the kafkaAutoRebalanceStatus filled by the KafkaReconciler

        Set<Integer> scalingUpAddedNodes = new HashSet<>();
        if (kafkaAutoRebalanceStatus != null && kafkaAutoRebalanceStatus.getModes() != null) {
            kafkaAutoRebalanceStatus.getModes().stream().filter(m -> m.getMode().equals(KafkaAutoRebalanceMode.ADD_BROKERS)).findFirst()
                    .ifPresent(m -> scalingUpAddedNodes.addAll(m.getBrokers()));
        }
        return new ScalingNodes(scalingDownBlockedNodes, scalingUpAddedNodes);
    }

    private Future<KafkaRebalance> getKafkaRebalance(String namespace, String cluster, KafkaAutoRebalanceMode kafkaAutoRebalanceMode) {
        return kafkaRebalanceOperator.getAsync(namespace, KafkaResources.autoRebalancingKafkaRebalanceResourceName(cluster, kafkaAutoRebalanceMode));
    }

    private Future<Boolean> createKafkaRebalance(String namespace, String cluster, KafkaAutoRebalanceMode kafkaAutoRebalanceMode, List<Integer> brokers) {
        Optional<KafkaAutoRebalanceConfiguration> autoRebalanceConfiguration =
                kafkaAutoRebalanceConfigurations.stream().filter(c -> c.getMode().equals(kafkaAutoRebalanceMode)).findFirst();
        if (autoRebalanceConfiguration.isEmpty()) {
            return Future.failedFuture(new RuntimeException("No auto-rebalancing configuration specified for mode " + kafkaAutoRebalanceMode));
        }

        if (autoRebalanceConfiguration.get().getTemplate() != null) {
            return kafkaRebalanceOperator.getAsync(namespace, autoRebalanceConfiguration.get().getTemplate().getName())
                    .compose(kafkaRebalanceTemplate -> {
                        if (kafkaRebalanceTemplate != null) {
                            KafkaRebalance kafkaRebalance = buildKafkaRebalance(kafkaRebalanceTemplate, namespace, cluster, kafkaAutoRebalanceMode, brokers);

                            LOGGER.infoCr(reconciliation, "Create KafkaRebalance {}/{} by using configuration from template {}/{}",
                                    kafkaRebalance.getMetadata().getNamespace(), kafkaRebalance.getMetadata().getName(),
                                    kafkaRebalanceTemplate.getMetadata().getNamespace(), kafkaRebalanceTemplate.getMetadata().getName());
                            return kafkaRebalanceOperator.createOrUpdate(reconciliation, kafkaRebalance)
                                    .map(true);
                        } else {
                            LOGGER.warnCr(reconciliation, "The specified KafkaRebalance template {}/{} for auto-rebalancing doesn't exist. Skipping auto-rebalancing.",
                                    namespace, autoRebalanceConfiguration.get().getTemplate().getName());
                            return Future.succeededFuture(false);
                        }
                    });
        } else {
            KafkaRebalance kafkaRebalance = buildKafkaRebalance(null, namespace, cluster, kafkaAutoRebalanceMode, brokers);

            LOGGER.infoCr(reconciliation, "Create KafkaRebalance {}/{} using default Cruise Control configuration. No template specified.",
                    kafkaRebalance.getMetadata().getNamespace(), kafkaRebalance.getMetadata().getName());
            return kafkaRebalanceOperator.createOrUpdate(reconciliation, kafkaRebalance)
                    .map(true);
        }
    }

    private KafkaRebalance buildKafkaRebalance(KafkaRebalance kafkaRebalanceTemplate, String namespace, String cluster, KafkaAutoRebalanceMode kafkaAutoRebalanceMode, List<Integer> brokers) {
        KafkaRebalanceBuilder builder = new KafkaRebalanceBuilder()
                .withNewMetadata()
                    .withNamespace(namespace)
                    .withName(KafkaResources.autoRebalancingKafkaRebalanceResourceName(cluster, kafkaAutoRebalanceMode))
                    .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, cluster)
                    .addToAnnotations(ANNO_STRIMZI_IO_REBALANCE_AUTOAPPROVAL, "true")
                    .addToFinalizers(STRIMZI_IO_AUTO_REBALANCING_FINALIZER)
                .endMetadata();

        KafkaRebalanceMode kafkaRebalanceMode;
        switch (kafkaAutoRebalanceMode) {
            case ADD_BROKERS -> kafkaRebalanceMode = KafkaRebalanceMode.ADD_BROKERS;
            case REMOVE_BROKERS -> kafkaRebalanceMode = KafkaRebalanceMode.REMOVE_BROKERS;
            default -> throw new IllegalArgumentException(kafkaAutoRebalanceMode + " is an invalid autorebalance mode");
        }

        // if specified, using the spec from the KafkaRebalance template
        if (kafkaRebalanceTemplate != null) {
            builder.withSpec(kafkaRebalanceTemplate.getSpec())
                    .editSpec()
                        .withMode(kafkaRebalanceMode)
                        .withBrokers(brokers)
                    .endSpec();
        } else {
            builder.withNewSpec()
                        .withMode(kafkaRebalanceMode)
                        .withBrokers(brokers)
                    .endSpec();
        }
        return builder.build();
    }

    private Future<Void> deleteKafkaRebalance(KafkaRebalance kafkaRebalance) {
        LOGGER.infoCr(reconciliation, "Delete KafkaRebalance {}/{}", kafkaRebalance.getMetadata().getNamespace(), kafkaRebalance.getMetadata().getName());
        // remove the finalizer to allow the deletion
        KafkaRebalance kafkaRebalancePatched = new KafkaRebalanceBuilder(kafkaRebalance)
                .editMetadata()
                    .removeFromFinalizers(STRIMZI_IO_AUTO_REBALANCING_FINALIZER)
                .endMetadata()
                .build();
        return kafkaRebalanceOperator.patchAsync(reconciliation, kafkaRebalancePatched)
                .compose(kr -> kafkaRebalanceOperator.deleteAsync(reconciliation, kr.getMetadata().getNamespace(), kr.getMetadata().getName(), false))
                .mapEmpty();
    }

    private Future<Void> refreshKafkaRebalance(KafkaRebalance kafkaRebalance, List<Integer> brokers) {
        LOGGER.infoCr(reconciliation, "Refresh KafkaRebalance {}/{}", kafkaRebalance.getMetadata().getNamespace(), kafkaRebalance.getMetadata().getName());
        KafkaRebalance kafkaRebalancePatched = new KafkaRebalanceBuilder(kafkaRebalance)
                .editMetadata()
                    .addToAnnotations(Map.of(ANNO_STRIMZI_IO_REBALANCE, KafkaRebalanceAnnotation.refresh.toString()))
                .endMetadata()
                .editSpec()
                    .withBrokers(brokers)
                .endSpec()
                .build();
        return kafkaRebalanceOperator.patchAsync(reconciliation, kafkaRebalancePatched)
                .mapEmpty();
    }

    private Future<Void> stopKafkaRebalance(KafkaRebalance kafkaRebalance) {
        LOGGER.infoCr(reconciliation, "Stop KafkaRebalance {}/{}", kafkaRebalance.getMetadata().getNamespace(), kafkaRebalance.getMetadata().getName());
        KafkaRebalance kafkaRebalancePatched = new KafkaRebalanceBuilder(kafkaRebalance)
                .editMetadata()
                    .addToAnnotations(Map.of(ANNO_STRIMZI_IO_REBALANCE, KafkaRebalanceAnnotation.stop.toString()))
                .endMetadata()
                .build();
        return kafkaRebalanceOperator.patchAsync(reconciliation, kafkaRebalancePatched)
                .mapEmpty();
    }

    private void updateStatus(KafkaAutoRebalanceStatus kafkaAutoRebalanceStatus, KafkaAutoRebalanceState state, ScalingNodes scalingNodes) {
        // just clear the modes field when there are no added nodes or blocked ones
        List<KafkaAutoRebalanceStatusBrokers> modes = null;
        if (!scalingNodes.blocked().isEmpty() || !scalingNodes.added().isEmpty()) {
            modes = new ArrayList<>(2);
            if (!scalingNodes.blocked().isEmpty()) {
                modes.add(
                        new KafkaAutoRebalanceStatusBrokersBuilder()
                                .withMode(KafkaAutoRebalanceMode.REMOVE_BROKERS)
                                .withBrokers(scalingNodes.blocked().stream().toList())
                                .build()
                );
            }
            if (!scalingNodes.added().isEmpty()) {
                modes.add(
                        new KafkaAutoRebalanceStatusBrokersBuilder()
                                .withMode(KafkaAutoRebalanceMode.ADD_BROKERS)
                                .withBrokers(scalingNodes.added().stream().toList())
                                .build()
                );
            }
        }
        kafkaAutoRebalanceStatus.setState(state);
        kafkaAutoRebalanceStatus.setLastTransitionTime(StatusUtils.iso8601Now());
        kafkaAutoRebalanceStatus.setModes(modes);
    }

    /**
     * Utility class to take the updated blocked nodes (on scaling down) and added nodes (on scaling up)
     */
    record ScalingNodes(Set<Integer> blocked, Set<Integer> added) {

        /**
         * @return true if both blocked scaled down or added on scale up nodes collections are empty, false otherwise
         */
        public boolean isEmpty() {
            return blocked().isEmpty() && added().isEmpty();
        }
    }
}
