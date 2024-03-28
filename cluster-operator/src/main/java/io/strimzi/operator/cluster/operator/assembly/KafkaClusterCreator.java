/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaMetadataConfigurationState;
import io.strimzi.operator.cluster.model.KafkaPool;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.KafkaVersionChange;
import io.strimzi.operator.cluster.model.SharedEnvironmentProvider;
import io.strimzi.operator.cluster.model.nodepools.NodePoolUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.StatusUtils;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    private final Vertx vertx;
    private final AdminClientProvider adminClientProvider;
    private final SecretOperator secretOperator;
    private final SharedEnvironmentProvider sharedEnvironmentProvider;
    private final BrokersInUseCheck brokerScaleDownOperations;
    private final KafkaMetadataConfigurationState kafkaMetadataConfigState;
    // State
    private boolean scaleDownCheckFailed = false;
    private boolean usedToBeBrokersCheckFailed = false;
    private final List<Condition> warningConditions = new ArrayList<>();

    /**
     * Constructor
     *
     * @param vertx                     Vert.x instance
     * @param reconciliation            Reconciliation marker
     * @param config                    Cluster Operator configuration
     * @param kafkaMetadataConfigState  Metadata state related to nodes configuration
     * @param supplier                  Resource Operators supplier
     */
    public KafkaClusterCreator(
            Vertx vertx,
            Reconciliation reconciliation,
            ClusterOperatorConfig config,
            KafkaMetadataConfigurationState kafkaMetadataConfigState,
            ResourceOperatorSupplier supplier
    ) {
        this.reconciliation = reconciliation;
        this.versions = config.versions();
        this.kafkaMetadataConfigState = kafkaMetadataConfigState;

        this.vertx = vertx;
        this.adminClientProvider = supplier.adminClientProvider;
        this.secretOperator = supplier.secretOperations;
        this.sharedEnvironmentProvider = supplier.sharedEnvironmentProvider;
        this.brokerScaleDownOperations = supplier.brokersInUseCheck;
    }

    /**
     * Prepares the Kafka Cluster model instance. It checks if any scale-down is happening and whether such scale-down
     * can be done. If it discovers any problems, it will try to fix them and create a fixed Kafka Cluster model
     * instance if the {@code tryToFixProblems} fag is set to true. It throws an exception otherwise.
     *
     * @param kafkaCr           Kafka custom resource
     * @param nodePools         List with Kafka Node Pool resources
     * @param oldStorage        Old storage configuration
     * @param currentPods       Existing Kafka pods
     * @param versionChange     Version Change object describing any possible upgrades / downgrades
     * @param kafkaStatus       The KafkaStatus where any possibly warnings will be added
     * @param tryToFixProblems  Flag indicating whether recoverable configuration issues should be fixed or not
     *
     * @return New Kafka Cluster instance
     */
    public Future<KafkaCluster> prepareKafkaCluster(
            Kafka kafkaCr,
            List<KafkaNodePool> nodePools,
            Map<String, Storage> oldStorage,
            Map<String, List<String>> currentPods,
            KafkaVersionChange versionChange,
            KafkaStatus kafkaStatus,
            boolean tryToFixProblems)   {
        return createKafkaCluster(kafkaCr, nodePools, oldStorage, currentPods, versionChange)
                .compose(kafka -> brokerRemovalCheck(kafkaCr, kafka))
                .compose(kafka -> {
                    if (checkFailed() && tryToFixProblems)   {
                        // We have a failure, and should try to fix issues
                        // Once we fix it, we call this method again, but this time with tryToFixProblems set to false
                        return revertScaleDown(kafka, kafkaCr, nodePools)
                                .compose(kafkaAndNodePools -> revertRoleChange(kafkaAndNodePools.kafkaCr(), kafkaAndNodePools.nodePoolCrs()))
                                .compose(kafkaAndNodePools -> prepareKafkaCluster(kafkaAndNodePools.kafkaCr(), kafkaAndNodePools.nodePoolCrs(), oldStorage, currentPods, versionChange, kafkaStatus, false));
                    } else if (checkFailed()) {
                        // We have a failure, but we should not try to fix it
                        List<String> errors = new ArrayList<>();

                        if (scaleDownCheckFailed)   {
                            errors.add("Cannot scale-down Kafka brokers " + kafka.removedNodes() + " because they have assigned partition-replicas.");
                        }

                        if (usedToBeBrokersCheckFailed) {
                            errors.add("Cannot remove the broker role from nodes " + kafka.usedToBeBrokerNodes() + " because they have assigned partition-replicas.");
                        }

                        return Future.failedFuture(new InvalidResourceException("Following errors were found when processing the Kafka custom resource: " + errors));
                    } else {
                        // If everything succeeded, we return the KafkaCluster object
                        // If any warning conditions exist from the reverted changes, we add them to the status
                        if (!warningConditions.isEmpty())   {
                            kafkaStatus.addConditions(warningConditions);
                        }

                        return Future.succeededFuture(kafka);
                    }
                });
    }

    /**
     * Creates a new Kafka cluster
     *
     * @param kafkaCr           Kafka custom resource
     * @param nodePoolCrs         List with KafkaNodePool custom resources
     * @param oldStorage        Old storage configuration
     * @param currentPods       Current Kafka pods
     * @param versionChange     Version change descriptor containing any upgrade / downgrade changes
     *
     * @return  Future with the new KafkaCluster object
     */
    private Future<KafkaCluster> createKafkaCluster(
            Kafka kafkaCr,
            List<KafkaNodePool> nodePoolCrs,
            Map<String, Storage> oldStorage,
            Map<String, List<String>> currentPods,
            KafkaVersionChange versionChange
    )   {
        return Future.succeededFuture(createKafkaCluster(reconciliation, kafkaCr, nodePoolCrs, oldStorage, currentPods, versionChange, kafkaMetadataConfigState, versions, sharedEnvironmentProvider));
    }

    /**
     * Checks if the broker scale down can be done or not based on whether the nodes are empty or still have some
     * partition-replicas assigned.
     *
     * @param kafkaCr   Kafka custom resource
     * @param kafka     Kafka cluster model
     *
     * @return  Future with the Kafka cluster model
     */
    private Future<KafkaCluster> brokerRemovalCheck(Kafka kafkaCr, KafkaCluster kafka) {
        if (skipBrokerScaleDownCheck(kafkaCr) // The check was disabled by the user
                || (kafka.removedNodes().isEmpty() && kafka.usedToBeBrokerNodes().isEmpty())) { // There is no scale-down or role change, so there is nothing to check
            scaleDownCheckFailed = false;
            usedToBeBrokersCheckFailed = false;
            return Future.succeededFuture(kafka);
        } else {
            return ReconcilerUtils.coTlsPemIdentity(reconciliation, secretOperator)
                    .compose(coTlsPemIdentity -> brokerScaleDownOperations.brokersInUse(reconciliation, vertx, coTlsPemIdentity, adminClientProvider))
                    .compose(brokersInUse -> {
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

                        return Future.succeededFuture(kafka);
                    });
        }
    }



    /**
     * Reverts the broker scale down if it is not allowed because the brokers are not empty
     *
     * @param kafka         Instance of the Kafka cluster model that contains information needed to revert the changes
     * @param kafkaCr       Kafka custom resource
     * @param nodePoolCrs   List with KafkaNodePool custom resources
     *
     * @return  Future with KafkaAndNodePools record containing the fixed Kafka and KafkaNodePool CRs
     */
    private Future<KafkaAndNodePools> revertScaleDown(KafkaCluster kafka, Kafka kafkaCr, List<KafkaNodePool> nodePoolCrs)   {
        if (scaleDownCheckFailed) {
            if (nodePoolCrs == null || nodePoolCrs.isEmpty()) {
                // There are no node pools => the Kafka CR is used
                int newReplicasCount = kafkaCr.getSpec().getKafka().getReplicas() + kafka.removedNodes().size();
                warningConditions.add(StatusUtils.buildWarningCondition("ScaleDownPreventionCheck", "Reverting scale-down of Kafka " + kafkaCr.getMetadata().getName() + " by changing number of replicas to " + newReplicasCount));
                LOGGER.warnCr(reconciliation, "Reverting scale-down of Kafka {} by changing number of replicas to {}", kafkaCr.getMetadata().getName(), newReplicasCount);

                Kafka newKafkaCr = new KafkaBuilder(kafkaCr)
                        .editSpec()
                        .editKafka()
                        .withReplicas(newReplicasCount)
                        .endKafka()
                        .endSpec()
                        .build();

                return Future.succeededFuture(new KafkaAndNodePools(newKafkaCr, nodePoolCrs));
            } else {
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

                return Future.succeededFuture(new KafkaAndNodePools(kafkaCr, newNodePools));
            }
        } else {
            // The scale-down check did not fail => return the original resources
            return Future.succeededFuture(new KafkaAndNodePools(kafkaCr, nodePoolCrs));
        }
    }

    /**
     * Reverts the role change when the broker role is removed from a node that has still assigned partition replicas
     *
     * @param kafkaCr       Kafka custom resource
     * @param nodePoolCrs   List with KafkaNodePool custom resources
     *
     * @return  Future with KafkaAndNodePools record containing the fixed Kafka and KafkaNodePool CRs
     */
    private Future<KafkaAndNodePools> revertRoleChange(Kafka kafkaCr, List<KafkaNodePool> nodePoolCrs)   {
        if (usedToBeBrokersCheckFailed) {
            List<KafkaNodePool> newNodePools = new ArrayList<>();

            for (KafkaNodePool nodePool : nodePoolCrs) {
                if (nodePool.getStatus() != null
                        && nodePool.getStatus().getRoles().contains(ProcessRoles.BROKER)
                        && !nodePool.getSpec().getRoles().contains(ProcessRoles.BROKER)) {
                    warningConditions.add(StatusUtils.buildWarningCondition("ScaleDownPreventionCheck", "Reverting role change of KafkaNodePool " + nodePool.getMetadata().getName() + " by adding the broker role to it"));
                    LOGGER.warnCr(reconciliation, "Reverting role change of KafkaNodePool {} by adding the broker role to it", nodePool.getMetadata().getName());
                    newNodePools.add(
                            new KafkaNodePoolBuilder(nodePool)
                                    .editSpec()
                                    .addToRoles(ProcessRoles.BROKER)
                                    .endSpec()
                                    .build());
                } else {
                    newNodePools.add(nodePool);
                }
            }

            return Future.succeededFuture(new KafkaAndNodePools(kafkaCr, newNodePools));
        } else {
            // The used-to-be-brokers check did not fail => return the original resources
            return Future.succeededFuture(new KafkaAndNodePools(kafkaCr, nodePoolCrs));
        }
    }

    /**
     * Utility method that checks if the scale-down check should be skipped or not.
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
        return scaleDownCheckFailed || usedToBeBrokersCheckFailed;
    }

    /**
     * Utility method for creating the Kafka cluster model. This uses a separate static method so that it can be also
     * used from tests and other places in the future.
     *
     * @param reconciliation                Reconciliation marker
     * @param kafkaCr                       Kafka custom resource
     * @param nodePoolCrs                   KafkaNodePool custom resources
     * @param oldStorage                    Old storage configuration
     * @param currentPods                   List of current Kafka pods
     * @param versionChange                 Version change descriptor containing any upgrade / downgrade changes
     * @param kafkaMetadataConfigState      Metadata state related to nodes configuration
     * @param versions                      List of supported Kafka versions
     * @param sharedEnvironmentProvider     Shared environment variables
     *
     * @return  New KafkaCluster object
     */
    public static KafkaCluster createKafkaCluster(
            Reconciliation reconciliation,
            Kafka kafkaCr,
            List<KafkaNodePool> nodePoolCrs,
            Map<String, Storage> oldStorage,
            Map<String, List<String>> currentPods,
            KafkaVersionChange versionChange,
            KafkaMetadataConfigurationState kafkaMetadataConfigState,
            KafkaVersion.Lookup versions,
            SharedEnvironmentProvider sharedEnvironmentProvider
    ) {
        // We prepare the KafkaPool models and create the KafkaCluster model
        // KRaft to be considered not only when fully enabled (KRAFT = 4) but also when a migration is about to start (PRE_MIGRATION = 1)
        // NOTE: this is important to drive the right validation happening in node pools (i.e. roles on node pools, storage, number of controllers, ...)
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(reconciliation, kafkaCr, nodePoolCrs, oldStorage, currentPods, kafkaMetadataConfigState.isPreMigrationToKRaft(), sharedEnvironmentProvider);
        String clusterId = kafkaMetadataConfigState.isPreMigrationToKRaft() ? NodePoolUtils.getOrGenerateKRaftClusterId(kafkaCr, nodePoolCrs) : NodePoolUtils.getClusterIdIfSet(kafkaCr, nodePoolCrs);
        return KafkaCluster.fromCrd(reconciliation, kafkaCr, pools, versions, versionChange, kafkaMetadataConfigState, clusterId, sharedEnvironmentProvider);
    }

    /**
     * Utility record to pass fixed custom resources between methods
     *
     * @param kafkaCr       Kafka custom resource
     * @param nodePoolCrs   List of KafkaNodePool resources
     */
    record KafkaAndNodePools(Kafka kafkaCr, List<KafkaNodePool> nodePoolCrs) { }
}
