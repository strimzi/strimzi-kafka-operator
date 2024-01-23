/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaPool;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.KafkaVersionChange;
import io.strimzi.operator.cluster.model.SharedEnvironmentProvider;
import io.strimzi.operator.cluster.model.nodepools.NodePoolUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Utility class to help to create the KafkaCluster model
 */
public class KafkaClusterCreator {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaClusterCreator.class.getName());

    // Settings
    private final Reconciliation reconciliation;
    private final boolean useKRaftFGEnabled;
    private final KafkaVersion.Lookup versions;

    // Operators and other tools
    private final Vertx vertx;
    private final AdminClientProvider adminClientProvider;
    private final SecretOperator secretOperator;
    private final SharedEnvironmentProvider sharedEnvironmentProvider;
    private final PreventBrokerScaleDownCheck brokerScaleDownOperations;

    // State
    private boolean scaleDownCheckFailed = false;

    /**
     * Constructor
     *
     * @param vertx             Vert.x instance
     * @param reconciliation    Reconciliation marker
     * @param config            Cluster Operator configuration
     * @param supplier          Resource Operators supplier
     */
    public KafkaClusterCreator(
            Vertx vertx,
            Reconciliation reconciliation,
            ClusterOperatorConfig config,
            ResourceOperatorSupplier supplier
    ) {
        this.reconciliation = reconciliation;
        this.versions = config.versions();
        this.useKRaftFGEnabled = config.featureGates().useKRaftEnabled();

        this.vertx = vertx;
        this.adminClientProvider = supplier.adminClientProvider;
        this.secretOperator = supplier.secretOperations;
        this.sharedEnvironmentProvider = supplier.sharedEnvironmentProvider;
        this.brokerScaleDownOperations = supplier.brokerScaleDownOperations;
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
     * @param tryToFixProblems  Flag indicating whether recoverable configuration issues should be fixed or not
     *
     * @return  New Kafka Cluster instance
     */
    public Future<KafkaCluster> prepareKafkaCluster(
            Kafka kafkaCr,
            List<KafkaNodePool> nodePools,
            Map<String, Storage> oldStorage,
            Map<String, List<String>> currentPods,
            KafkaVersionChange versionChange,
            boolean tryToFixProblems
    )   {
        return createKafkaCluster(kafkaCr, nodePools, oldStorage, currentPods, versionChange)
                .compose(kafka -> brokerScaleDownCheck(kafkaCr, kafka))
                .compose(kafka -> {
                    if (scaleDownCheckFailed && tryToFixProblems)   {
                        // We have a failure, and should try to fix issues
                        return revertScaleDown(kafka, kafkaCr, nodePools)
                                .compose(kafkaAndNodePools -> prepareKafkaCluster(kafkaAndNodePools.kafkaCr(), kafkaAndNodePools.nodePools(), oldStorage, currentPods, versionChange, false));
                    } else if (scaleDownCheckFailed) {
                        // We have a failure, but we should not try to fix it
                        return Future.failedFuture(new InvalidResourceException("Cannot scale-down Kafka brokers because they have assigned partition-replicas."));
                    } else {
                        // If something else failed, we just re-throw it
                        return Future.succeededFuture(kafka);
                    }
                });
    }

    /**
     * Creates a new Kafka cluster
     *
     * @param kafkaCr           Kafka custom resource
     * @param nodePools         List with KafkaNodePool custom resources
     * @param oldStorage        Old storage configuration
     * @param currentPods       Current Kafka pods
     * @param versionChange     Version change descriptor containing any upgrade / downgrade changes
     *
     * @return  Future with the new KafkaCluster object
     */
    private Future<KafkaCluster> createKafkaCluster(
            Kafka kafkaCr,
            List<KafkaNodePool> nodePools,
            Map<String, Storage> oldStorage,
            Map<String, List<String>> currentPods,
            KafkaVersionChange versionChange
    )   {
        return Future.succeededFuture(createKafkaCluster(reconciliation, kafkaCr, nodePools, oldStorage, currentPods, versionChange, useKRaftFGEnabled, versions, sharedEnvironmentProvider));
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
    private Future<KafkaCluster> brokerScaleDownCheck(Kafka kafkaCr, KafkaCluster kafka) {
        if (skipBrokerScaleDownCheck(kafkaCr) || kafka.removedNodes().isEmpty()) {
            scaleDownCheckFailed = false;
            return Future.succeededFuture(kafka);
        } else {
            return brokerScaleDownOperations.canScaleDownBrokers(reconciliation, vertx, kafka.removedNodes(), secretOperator, adminClientProvider)
                    .compose(brokersContainingPartitions -> {
                        if (!brokersContainingPartitions.isEmpty()) {
                            LOGGER.warnCr(reconciliation, "Cannot scale down brokers {} because {} have assigned partition-replicas", kafka.removedNodes(), brokersContainingPartitions);
                            scaleDownCheckFailed = true;
                        } else {
                            scaleDownCheckFailed = false;
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
     * @param nodePools     List with KafkaNodePool custom resources
     *
     * @return  Future with KafkaAndNodePools record containing the fixed Kafka and KafkaNodePool CRs
     */
    private Future<KafkaAndNodePools> revertScaleDown(KafkaCluster kafka, Kafka kafkaCr, List<KafkaNodePool> nodePools)   {
        if (nodePools == null || nodePools.isEmpty())    {
            // There are no node pools => the Kafka CR is used
            int newReplicasCount = kafkaCr.getSpec().getKafka().getReplicas() + kafka.removedNodes().size();
            LOGGER.warnCr(reconciliation, "Reverting scale-down of Kafka {} by changing number of replicas to {}", kafkaCr.getMetadata().getName(), newReplicasCount);

            Kafka newKafkaCr = new KafkaBuilder(kafkaCr)
                    .editSpec()
                        .editKafka()
                            .withReplicas(newReplicasCount)
                        .endKafka()
                    .endSpec()
                    .build();

            return Future.succeededFuture(new KafkaAndNodePools(newKafkaCr, nodePools));
        } else {
            // Node pools are used -> we have to fix scale down in the KafkaNodePools
            List<KafkaNodePool> newNodePools = new ArrayList<>();

            for (KafkaNodePool nodePool : nodePools)    {
                if (nodePool.getStatus() != null
                        && nodePool.getStatus().getNodeIds() != null
                        && nodePool.getSpec().getReplicas() < nodePool.getStatus().getNodeIds().size())  {
                    int newReplicasCount = nodePool.getStatus().getNodeIds().size();
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
     * Utility method for creating the Kafka cluster model. This uses a separate static method so that it can be also
     * used from tests and other places in the future.
     *
     * @param reconciliation                Reconciliation marker
     * @param kafkaCr                       Kafka custom resource
     * @param nodePools                     KafkaNodePool custom resources
     * @param oldStorage                    Old storage configuration
     * @param currentPods                   List of current Kafka pods
     * @param versionChange                 Version change descriptor containing any upgrade / downgrade changes
     * @param useKRaftEnabled               Flag indicating if UseKRaft feature gate is enabled or not
     * @param versions                      List of supported Kafka versions
     * @param sharedEnvironmentProvider     Shared environment variables
     *
     * @return  New KafkaCluster object
     */
    static KafkaCluster createKafkaCluster(
            Reconciliation reconciliation,
            Kafka kafkaCr,
            List<KafkaNodePool> nodePools,
            Map<String, Storage> oldStorage,
            Map<String, List<String>> currentPods,
            KafkaVersionChange versionChange,
            boolean useKRaftEnabled,
            KafkaVersion.Lookup versions,
            SharedEnvironmentProvider sharedEnvironmentProvider
    ) {
        boolean isKRaftEnabled = useKRaftEnabled && ReconcilerUtils.kraftEnabled(kafkaCr);
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(reconciliation, kafkaCr, nodePools, oldStorage, currentPods, isKRaftEnabled, sharedEnvironmentProvider);
        String clusterId = isKRaftEnabled ? NodePoolUtils.getOrGenerateKRaftClusterId(kafkaCr, nodePools) : NodePoolUtils.getClusterIdIfSet(kafkaCr, nodePools);
        return KafkaCluster.fromCrd(reconciliation, kafkaCr, pools, versions, versionChange, isKRaftEnabled, clusterId, sharedEnvironmentProvider);
    }

    /**
     * Utility record to pass fixed custom resources between methods
     *
     * @param kafkaCr       Kafka custom resource
     * @param nodePools     List of KafkaNodePool resources
     */
    record KafkaAndNodePools(Kafka kafkaCr, List<KafkaNodePool> nodePools) { }
}
