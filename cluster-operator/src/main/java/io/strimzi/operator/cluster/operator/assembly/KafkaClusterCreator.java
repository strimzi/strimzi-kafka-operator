/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaPool;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.KafkaVersionChange;
import io.strimzi.operator.cluster.model.SharedEnvironmentProvider;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.KafkaClusterSecurityContext;
import io.strimzi.operator.cluster.model.nodepools.NodePoolUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.StatusUtils;
import io.strimzi.operator.common.operator.resource.kubernetes.SecretOperator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

/**
 * Utility class to help to create the KafkaCluster model
 */
public class KafkaClusterCreator {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaClusterCreator.class.getName());

    // Settings
    private final Reconciliation reconciliation;
    private final KafkaVersion.Lookup versions;

    // Operators and other tools
    private final AdminClientProvider adminClientProvider;
    private final SecretOperator secretOperator;
    private final SharedEnvironmentProvider sharedEnvironmentProvider;
    private final BrokersInUseCheck brokerScaleDownOperations;
    // State
    private boolean scaleDownCheckFailed = false;
    private boolean usedToBeBrokersCheckFailed = false;
    private boolean volumeRemovalCheckFailed = false;
    private final List<Condition> warningConditions = new ArrayList<>();
    private final Set<Integer> scalingDownBlockedNodes = new HashSet<>();
    private BrokersInUseCheck.VolumesInUse volumesInUse = new BrokersInUseCheck.VolumesInUse(Map.of(), Map.of());

    /**
     * Constructor
     *
     * @param reconciliation    Reconciliation marker
     * @param config            Cluster Operator configuration
     * @param supplier          Resource Operators supplier
     */
    public KafkaClusterCreator(
            Reconciliation reconciliation,
            ClusterOperatorConfig config,
            ResourceOperatorSupplier supplier
    ) {
        this.reconciliation = reconciliation;
        this.versions = config.versions();

        this.adminClientProvider = supplier.adminClientProvider;
        this.secretOperator = supplier.secretOperations;
        this.sharedEnvironmentProvider = supplier.sharedEnvironmentProvider;
        this.brokerScaleDownOperations = supplier.brokersInUseCheck;
    }

    /**
     * Gets the nodes blocked for scaling down to be considered for auto-rebalancing before removal.
     *
     * @return the nodes blocked for scaling down to be considered for an auto-rebalancing,
     * before they are actually removed from the cluster
     */
    public Set<Integer> scalingDownBlockedNodes() {
        return scalingDownBlockedNodes;
    }

    /**
     * Prepares the Kafka Cluster model instance. It checks if any scale-down is happening and whether such scale-down
     * can be done. If it discovers any problems, it will try to fix them and create a fixed Kafka Cluster model
     * instance if the {@code tryToFixProblems} fag is set to true. It throws an exception otherwise.
     *
     * @param kafkaCr           Kafka custom resource
     * @param nodePools         List with Kafka Node Pool resources
     * @param oldStorage        Old storage configuration
     * @param versionChange     Version Change object describing any possible upgrades / downgrades
     * @param kafkaStatus       The KafkaStatus where any possibly warnings will be added
     * @param tryToFixProblems  Flag indicating whether recoverable configuration issues should be fixed or not
     * @param securityContext   Security context for the Kafka cluster
     *
     * @return  New Kafka Cluster instance
     */
    public CompletionStage<KafkaCluster> prepareKafkaCluster(
            Kafka kafkaCr,
            List<KafkaNodePool> nodePools,
            Map<String, Storage> oldStorage,
            KafkaVersionChange versionChange,
            KafkaStatus kafkaStatus,
            boolean tryToFixProblems,
            KafkaClusterSecurityContext securityContext)   {
        return createKafkaCluster(kafkaCr, nodePools, oldStorage, versionChange, securityContext)
                .thenCompose(kafka -> brokerRemovalCheck(kafkaCr, kafka))
                .thenCompose(kafka -> volumeRemovalCheck(kafkaCr, kafka))
                .thenCompose(kafka -> {
                    if (checkFailed() && tryToFixProblems)   {
                        if (scaleDownCheckFailed || usedToBeBrokersCheckFailed) {
                            // saving scaling down blocked nodes, before they are reverted back
                            this.scalingDownBlockedNodes.addAll(kafka.removedNodes());
                        }
                        // We have a failure, and should try to fix issues
                        // Once we fix it, we call this method again, but this time with tryToFixProblems set to false
                        return revertScaleDown(nodePools)
                                .thenCompose(revertedNodePools -> revertRoleChange(revertedNodePools))
                                .thenCompose(revertedNodePools -> revertVolumeRemoval(revertedNodePools, kafkaCr, oldStorage))
                                .thenCompose(revertedNodePools -> prepareKafkaCluster(kafkaCr, revertedNodePools, oldStorage, versionChange, kafkaStatus, false, securityContext));
                    } else if (checkFailed()) {
                        // We have a failure, but we should not try to fix it
                        List<String> errors = new ArrayList<>();

                        if (scaleDownCheckFailed)   {
                            errors.add("Cannot scale-down Kafka brokers " + kafka.removedNodes() + " because they have assigned partition-replicas.");
                        }

                        if (usedToBeBrokersCheckFailed) {
                            errors.add("Cannot remove the broker role from nodes " + kafka.usedToBeBrokerNodes() + " because they have assigned partition-replicas.");
                        }

                        if (!volumesInUse.notEmpty().isEmpty()) {
                            errors.add("Cannot remove the " + blockedVolumes(volumesInUse.notEmpty()) + " because they have assigned partition-replicas.");
                        }

                        if (!volumesInUse.notChecked().isEmpty()) {
                            errors.add("Cannot remove the " + blockedVolumes(volumesInUse.notChecked()) + " because it is not known whether they are empty. The broker did not answer, or the log directory is offline.");
                        }

                        return CompletableFuture.failedFuture(new InvalidResourceException("Following errors were found when processing the Kafka custom resource: " + errors));
                    } else {
                        // If everything succeeded, we return the KafkaCluster object
                        // If any warning conditions exist from the reverted changes, we add them to the status
                        if (!warningConditions.isEmpty())   {
                            kafkaStatus.addConditions(warningConditions);
                        }

                        return CompletableFuture.completedFuture(kafka);
                    }
                });
    }

    /**
     * Creates a new Kafka cluster
     *
     * @param kafkaCr           Kafka custom resource
     * @param nodePoolCrs       List with KafkaNodePool custom resources
     * @param oldStorage        Old storage configuration
     * @param versionChange     Version change descriptor containing any upgrade / downgrade changes
     * @param securityContext   Security context for the Kafka cluster
     *
     * @return  CompletionStage with the new KafkaCluster object
     */
    private CompletionStage<KafkaCluster> createKafkaCluster(
            Kafka kafkaCr,
            List<KafkaNodePool> nodePoolCrs,
            Map<String, Storage> oldStorage,
            KafkaVersionChange versionChange,
            KafkaClusterSecurityContext securityContext
    )   {
        return CompletableFuture.completedFuture(createKafkaCluster(reconciliation, kafkaCr, nodePoolCrs, oldStorage, versionChange, versions, sharedEnvironmentProvider, securityContext));
    }

    /**
     * Checks if the broker scale down can be done or not based on whether the nodes are empty or still have some
     * partition-replicas assigned.
     *
     * @param kafkaCr   Kafka custom resource
     * @param kafka     Kafka cluster model
     *
     * @return  CompletionStage with the Kafka cluster model
     */
    private CompletionStage<KafkaCluster> brokerRemovalCheck(Kafka kafkaCr, KafkaCluster kafka) {
        if (skipBrokerScaleDownCheck(kafkaCr) // The check was disabled by the user
                || (kafka.removedNodes().isEmpty() && kafka.usedToBeBrokerNodes().isEmpty())) { // There is no scale-down or role change, so there is nothing to check
            scaleDownCheckFailed = false;
            usedToBeBrokersCheckFailed = false;
            return CompletableFuture.completedFuture(kafka);
        } else {
            return ReconcilerUtils.coIdentity(reconciliation, secretOperator, kafka.securityContext())
                    .toCompletionStage()
                    .thenCompose(coTlsPemIdentity -> brokerScaleDownOperations.brokersInUse(reconciliation, coTlsPemIdentity, adminClientProvider))
                    .thenApply(brokersInUse -> {
                        // Check nodes that are being scaled down
                        Set<Integer> scaledDownBrokersInUse = kafka.removedNodes().stream().filter(brokersInUse::contains).collect(Collectors.toSet());
                        if (!scaledDownBrokersInUse.isEmpty()) {
                            LOGGER.warnCr(reconciliation, "Cannot scale down brokers {} because {} have assigned partition-replicas", kafka.removedNodes(), scaledDownBrokersInUse);
                            scaleDownCheckFailed = true;
                        } else {
                            scaleDownCheckFailed = false;
                        }

                        // Check nodes that used to have broker role but should not have it anymore
                        Set<Integer> usedToBeBrokersInUse = kafka.usedToBeBrokerNodes().stream().filter(brokersInUse::contains).collect(Collectors.toSet());
                        if (!usedToBeBrokersInUse.isEmpty()) {
                            LOGGER.warnCr(reconciliation, "Cannot remove the broker role from nodes {} because {} have still assigned partition-replicas", kafka.usedToBeBrokerNodes(), usedToBeBrokersInUse);
                            usedToBeBrokersCheckFailed = true;
                        } else {
                            usedToBeBrokersCheckFailed = false;
                        }

                        return kafka;
                    });
        }
    }

    /**
     * Checks if the JBOD volumes which are being removed are empty or if they still have some partition-replicas
     * assigned.
     *
     * @param kafkaCr   Kafka custom resource
     * @param kafka     Kafka cluster model
     *
     * @return  CompletionStage with the Kafka cluster model
     */
    private CompletionStage<KafkaCluster> volumeRemovalCheck(Kafka kafkaCr, KafkaCluster kafka) {
        Map<Integer, Set<Integer>> removedVolumes = kafka.removedJbodVolumes();

        if (skipBrokerScaleDownCheck(kafkaCr) // The check was disabled by the user
                || removedVolumes.isEmpty()) { // There are no removed volumes, so there is nothing to check
            volumeRemovalCheckFailed = false;
            volumesInUse = new BrokersInUseCheck.VolumesInUse(Map.of(), Map.of());
            return CompletableFuture.completedFuture(kafka);
        } else {
            return ReconcilerUtils.coIdentity(reconciliation, secretOperator, kafka.securityContext())
                    .toCompletionStage()
                    .thenCompose(coTlsPemIdentity -> brokerScaleDownOperations.volumesInUse(reconciliation, coTlsPemIdentity, adminClientProvider, removedVolumes))
                    .thenApply(result -> {
                        volumesInUse = result;
                        volumeRemovalCheckFailed = !result.nothingBlocked();

                        if (!result.notEmpty().isEmpty()) {
                            LOGGER.warnCr(reconciliation, "Cannot remove the {} because they have assigned partition-replicas", blockedVolumes(result.notEmpty()));
                        }

                        if (!result.notChecked().isEmpty()) {
                            LOGGER.warnCr(reconciliation, "Cannot remove the {} because it is not known whether they are empty. The broker did not answer, or the log directory is offline", blockedVolumes(result.notChecked()));
                        }

                        return kafka;
                    });
        }
    }

    /**
     * Reverts the broker scale down if it is not allowed because the brokers are not empty
     *
     * @param nodePoolCrs   List with KafkaNodePool custom resources
     *
     * @return  CompletionStage with KafkaAndNodePools record containing the fixed Kafka and KafkaNodePool CRs
     */
    private CompletionStage<List<KafkaNodePool>> revertScaleDown(List<KafkaNodePool> nodePoolCrs)   {
        if (scaleDownCheckFailed) {
            // Node pools are used -> we have to fix scale down in the KafkaNodePools
            List<KafkaNodePool> newNodePools = new ArrayList<>();

            for (KafkaNodePool nodePool : nodePoolCrs) {
                if (nodePool.getStatus() != null
                        && nodePool.getStatus().getRoles().contains(ProcessRoles.BROKER)
                        && nodePool.getStatus().getNodeIds() != null
                        && nodePool.getSpec().getReplicas() < nodePool.getStatus().getNodeIds().size()) {
                    int newReplicasCount = nodePool.getStatus().getNodeIds().size();
                    warningConditions.add(StatusUtils.buildWarningCondition("ScaleDownPreventionCheck", "Reverting scale-down of KafkaNodePool " + nodePool.getMetadata().getName() + " by changing number of replicas to " + newReplicasCount));
                    LOGGER.warnCr(reconciliation, "Reverting scale-down of KafkaNodePool {} by changing number of replicas to {}", nodePool.getMetadata().getName(), newReplicasCount);
                    newNodePools.add(
                            new KafkaNodePoolBuilder(nodePool)
                                    .editSpec()
                                        .withReplicas(newReplicasCount)
                                    .endSpec()
                                    .build());
                } else {
                    newNodePools.add(nodePool);
                }
            }

            return CompletableFuture.completedFuture(newNodePools);
        } else {
            // The scale-down check did not fail => return the original resources
            return CompletableFuture.completedFuture(nodePoolCrs);
        }
    }

    /**
     * Reverts the role change when the broker role is removed from a node that has still assigned partition replicas
     *
     * @param nodePoolCrs   List with KafkaNodePool custom resources
     *
     * @return  CompletionStage with KafkaAndNodePools record containing the fixed Kafka and KafkaNodePool CRs
     */
    private CompletionStage<List<KafkaNodePool>> revertRoleChange(List<KafkaNodePool> nodePoolCrs)   {
        if (usedToBeBrokersCheckFailed) {
            List<KafkaNodePool> newNodePools = new ArrayList<>();

            for (KafkaNodePool nodePool : nodePoolCrs) {
                if (nodePool.getStatus() != null
                        && nodePool.getStatus().getRoles().contains(ProcessRoles.BROKER)
                        && !nodePool.getSpec().getRoles().contains(ProcessRoles.BROKER)) {
                    warningConditions.add(StatusUtils.buildWarningCondition("ScaleDownPreventionCheck", "Reverting role change of KafkaNodePool " + nodePool.getMetadata().getName() + " (setting roles to " + nodePool.getStatus().getRoles() + ")"));
                    LOGGER.warnCr(reconciliation, "Reverting role change of KafkaNodePool {} (setting roles to {})", nodePool.getMetadata().getName(), nodePool.getStatus().getRoles());
                    newNodePools.add(
                            new KafkaNodePoolBuilder(nodePool)
                                    .editSpec()
                                        .withRoles(nodePool.getStatus().getRoles())
                                    .endSpec()
                                    .build());
                } else {
                    newNodePools.add(nodePool);
                }
            }

            return CompletableFuture.completedFuture(newNodePools);
        } else {
            // The used-to-be-brokers check did not fail => return the original resources
            return CompletableFuture.completedFuture(nodePoolCrs);
        }
    }

    /**
     * Reverts the removal of the JBOD volumes if it is not allowed because the volumes are not empty. The whole
     * storage configuration of the affected node pools is set back to the storage the Kafka cluster runs on, in the
     * same way as when the storage change is rejected in {@code KafkaPool.fromCrd}.
     *
     * @param nodePoolCrs   List with KafkaNodePool custom resources
     * @param kafkaCr       Kafka custom resource
     * @param oldStorage    Old storage configuration
     *
     * @return  CompletionStage with the list of the fixed KafkaNodePool CRs
     */
    private CompletionStage<List<KafkaNodePool>> revertVolumeRemoval(List<KafkaNodePool> nodePoolCrs, Kafka kafkaCr, Map<String, Storage> oldStorage) {
        if (volumeRemovalCheckFailed) {
            List<KafkaNodePool> newNodePools = new ArrayList<>();

            for (KafkaNodePool nodePool : nodePoolCrs) {
                Storage currentStorage = oldStorage.get(KafkaPool.componentName(kafkaCr, nodePool));

                if (currentStorage != null
                        && nodePool.getStatus() != null
                        && nodePool.getStatus().getNodeIds() != null
                        && nodePool.getStatus().getNodeIds().stream().anyMatch(this::isBlocked)) {
                    String blockedReason = blockedReason(nodePool);
                    warningConditions.add(StatusUtils.buildWarningCondition("ScaleDownPreventionCheck", "Reverting all storage changes of KafkaNodePool " + nodePool.getMetadata().getName() + " because they remove JBOD volumes which " + blockedReason));
                    LOGGER.warnCr(reconciliation, "Reverting all storage changes of KafkaNodePool {} because they remove JBOD volumes which {}", nodePool.getMetadata().getName(), blockedReason);
                    newNodePools.add(
                            new KafkaNodePoolBuilder(nodePool)
                                    .editSpec()
                                        .withStorage(currentStorage)
                                    .endSpec()
                                    .build());
                } else {
                    newNodePools.add(nodePool);
                }
            }

            return CompletableFuture.completedFuture(newNodePools);
        } else {
            // The volume removal check did not fail => return the original resources
            return CompletableFuture.completedFuture(nodePoolCrs);
        }
    }

    /**
     * Describes why the JBOD volume removal was blocked for the given node pool. Both reasons are listed when
     * different nodes of the pool are blocked for different reasons.
     *
     * @param nodePool  KafkaNodePool custom resource
     *
     * @return  Text saying whether the volumes hold partition replicas or could not be checked
     */
    private String blockedReason(KafkaNodePool nodePool) {
        List<String> reasons = new ArrayList<>();

        if (nodePool.getStatus().getNodeIds().stream().anyMatch(volumesInUse.notEmpty()::containsKey)) {
            reasons.add("are not empty");
        }

        if (nodePool.getStatus().getNodeIds().stream().anyMatch(volumesInUse.notChecked()::containsKey)) {
            reasons.add("could not be checked, because a broker did not answer or a log directory is offline");
        }

        return String.join(" or ", reasons);
    }

    /**
     * Checks whether the JBOD volume removal was blocked on the given Kafka node.
     *
     * @param nodeId    ID of the Kafka node
     *
     * @return  True when the removal was blocked. False otherwise.
     */
    private boolean isBlocked(Integer nodeId) {
        return volumesInUse.notEmpty().containsKey(nodeId) || volumesInUse.notChecked().containsKey(nodeId);
    }

    /**
     * Describes the blocked volumes. Nodes which are blocked on the same volumes are listed together, so that the
     * volumes are never paired with a node which does not have them.
     *
     * @param volumesPerNode    Map with the node IDs and their JBOD volume IDs
     *
     * @return  Text such as "JBOD volumes [1] from Kafka brokers [1000, 1001]"
     */
    private static String blockedVolumes(Map<Integer, Set<Integer>> volumesPerNode) {
        Map<Set<Integer>, Set<Integer>> nodesPerVolumes = new LinkedHashMap<>();

        for (Map.Entry<Integer, Set<Integer>> node : volumesPerNode.entrySet()) {
            nodesPerVolumes.computeIfAbsent(node.getValue(), volumes -> new LinkedHashSet<>()).add(node.getKey());
        }

        return nodesPerVolumes.entrySet()
                .stream()
                .map(entry -> "JBOD volumes " + entry.getKey() + " from Kafka brokers " + entry.getValue())
                .collect(Collectors.joining(", "));
    }

    /**
     * Utility method that checks if the checks preventing data loss should be skipped or not.
     *
     * @param kafkaCr   Kafka custom resource
     *
     * @return  True if the check should be skipped. False otherwise.
     */
    private static boolean skipBrokerScaleDownCheck(Kafka kafkaCr)  {
        return Annotations.booleanAnnotation(kafkaCr, Annotations.ANNO_STRIMZI_IO_SKIP_BROKER_SCALEDOWN_CHECK, false);
    }

    /**
     * Checks if there were any failures during the validation
     *
     * @return  True if any checks failed. False otherwise.
     */
    private boolean checkFailed()   {
        return scaleDownCheckFailed || usedToBeBrokersCheckFailed || volumeRemovalCheckFailed;
    }

    /**
     * Utility method for creating the Kafka cluster model. This uses a separate static method so that it can be also
     * used from tests and other places in the future.
     *
     * @param reconciliation                Reconciliation marker
     * @param kafkaCr                       Kafka custom resource
     * @param nodePoolCrs                   KafkaNodePool custom resources
     * @param oldStorage                    Old storage configuration
     * @param versionChange                 Version change descriptor containing any upgrade / downgrade changes
     * @param versions                      List of supported Kafka versions
     * @param sharedEnvironmentProvider     Shared environment variables
     * @param securityContext               Security context for the Kafka cluster
     *
     * @return  New KafkaCluster object
     */
    public static KafkaCluster createKafkaCluster(
            Reconciliation reconciliation,
            Kafka kafkaCr,
            List<KafkaNodePool> nodePoolCrs,
            Map<String, Storage> oldStorage,
            KafkaVersionChange versionChange,
            KafkaVersion.Lookup versions,
            SharedEnvironmentProvider sharedEnvironmentProvider,
            KafkaClusterSecurityContext securityContext) {
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(reconciliation, kafkaCr, nodePoolCrs, oldStorage, versionChange, sharedEnvironmentProvider);
        String clusterId = NodePoolUtils.getOrGenerateKRaftClusterId(kafkaCr, nodePoolCrs);
        return KafkaCluster.fromCrd(reconciliation, kafkaCr, pools, versions, versionChange, clusterId, sharedEnvironmentProvider, securityContext);
    }
}
