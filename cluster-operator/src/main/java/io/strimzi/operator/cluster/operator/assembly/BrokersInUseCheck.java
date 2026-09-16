/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.VolumeUtils;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.auth.Identity;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

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
 * Class which contains several utility function which check if broker scale down, role change or removal of a JBOD
 * volume can be done or not.
 */
public class BrokersInUseCheck {
    /**
     * Logger
     */
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(BrokersInUseCheck.class.getName());

    /**
     * Constructor
     */
    public BrokersInUseCheck() { }

    /**
     * Checks if broker contains any partition replicas when scaling down
     *
     * @param reconciliation        Reconciliation marker
     * @param coIdentity            Trust set and identity for authentication for connecting to the Kafka cluster
     * @param adminClientProvider   Used to create the Admin client instance
     *
     * @return returns CompletionStage with set of node ids containing partition replicas based on the outcome of the check
     */
    public CompletionStage<Set<Integer>> brokersInUse(Reconciliation reconciliation, Identity coIdentity, AdminClientProvider adminClientProvider) {
        try {
            Admin kafkaAdmin = createAdminClient(reconciliation, coIdentity, adminClientProvider);

            return topicNames(kafkaAdmin)
                    .thenCompose(names -> describeTopics(kafkaAdmin, names))
                    .thenApply(topicDescriptions -> {
                        Set<Integer> brokersWithPartitionReplicas = new HashSet<>();

                        for (TopicDescription td : topicDescriptions.values()) {
                            for (TopicPartitionInfo pd : td.partitions()) {
                                for (Node broker : pd.replicas()) {
                                    brokersWithPartitionReplicas.add(broker.id());
                                }
                            }
                        }

                        return brokersWithPartitionReplicas;
                    }).whenComplete((result, error) -> {
                        if (error != null) {
                            LOGGER.warnCr(reconciliation, "Failed to get list of brokers in use", error);
                        }
                        kafkaAdmin.close();
                    });
        } catch (KafkaException e) {
            LOGGER.warnCr(reconciliation, "Failed to check if broker contains any partition replicas", e);
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Result of the JBOD volume check.
     *
     * @param notEmpty      Map of the node IDs and the removed volumes which still contain partition replicas
     * @param notChecked    Map of the node IDs and the removed volumes which could not be checked
     */
    public record VolumesInUse(Map<Integer, Set<Integer>> notEmpty, Map<Integer, Set<Integer>> notChecked) {
        /**
         * Checks whether the removal can go ahead.
         *
         * @return  True when no volume is blocked. False otherwise.
         */
        public boolean nothingBlocked() {
            return notEmpty.isEmpty() && notChecked.isEmpty();
        }
    }

    /**
     * Checks which of the JBOD volumes that are about to be removed still contain partition replicas.
     *
     * @param reconciliation        Reconciliation marker
     * @param coIdentity            Trust set and identity for authentication for connecting to the Kafka cluster
     * @param adminClientProvider   Used to create the Admin client instance
     * @param removedVolumes        Map with the broker node IDs and the IDs of the JBOD volumes removed from them
     *
     * @return  CompletionStage with the volumes which are not empty and the volumes which could not be checked
     */
    public CompletionStage<VolumesInUse> volumesInUse(Reconciliation reconciliation, Identity coIdentity, AdminClientProvider adminClientProvider, Map<Integer, Set<Integer>> removedVolumes) {
        Admin kafkaAdmin;

        try {
            kafkaAdmin = createAdminClient(reconciliation, coIdentity, adminClientProvider);
        } catch (KafkaException e) {
            LOGGER.warnCr(reconciliation, "Failed to create the Admin client", e);
            return CompletableFuture.failedFuture(e);
        }

        try {
            Map<Integer, CompletionStage<Map<String, LogDirDescription>>> logDirs = describeLogDirs(kafkaAdmin, removedVolumes.keySet());

            List<CompletionStage<NodeResult>> checks = removedVolumes.entrySet()
                    .stream()
                    .map(node -> checkNode(reconciliation, node.getKey(), node.getValue(), logDirs.get(node.getKey())))
                    .toList();

            return CompletableFuture.allOf(checks.stream().map(CompletionStage::toCompletableFuture).toArray(CompletableFuture[]::new))
                    .thenApply(i -> {
                        Map<Integer, Set<Integer>> notEmpty = new LinkedHashMap<>();
                        Map<Integer, Set<Integer>> notChecked = new LinkedHashMap<>();

                        for (CompletionStage<NodeResult> check : checks) {
                            NodeResult result = check.toCompletableFuture().join();

                            if (!result.notEmpty().isEmpty())    {
                                notEmpty.put(result.nodeId(), result.notEmpty());
                            }

                            if (!result.notChecked().isEmpty())  {
                                notChecked.put(result.nodeId(), result.notChecked());
                            }
                        }

                        return new VolumesInUse(notEmpty, notChecked);
                    })
                    .whenComplete((result, error) -> {
                        if (error != null) {
                            LOGGER.warnCr(reconciliation, "Failed to get the list of volumes in use", error);
                        }
                        kafkaAdmin.close();
                    });
        } catch (KafkaException e) {
            kafkaAdmin.close();
            LOGGER.warnCr(reconciliation, "Failed to check if the removed volumes contain any partition replicas", e);
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Result of the check for one Kafka node.
     *
     * @param nodeId        ID of the Kafka node
     * @param notEmpty      IDs of the removed volumes which still contain partition replicas
     * @param notChecked    IDs of the removed volumes which could not be checked
     */
    private record NodeResult(Integer nodeId, Set<Integer> notEmpty, Set<Integer> notChecked) { }

    /**
     * Checks the log directories of a single Kafka node.
     *
     * @param reconciliation    Reconciliation marker
     * @param nodeId            ID of the Kafka node
     * @param volumeIds         IDs of the JBOD volumes removed from this node
     * @param logDirs           CompletionStage with the log directories reported by this node
     *
     * @return  CompletionStage with the result for this node
     */
    private CompletionStage<NodeResult> checkNode(Reconciliation reconciliation, Integer nodeId, Set<Integer> volumeIds, CompletionStage<Map<String, LogDirDescription>> logDirs) {
        if (logDirs == null) {
            LOGGER.warnCr(reconciliation, "Kafka node {} was not described, so it is not possible to check if its volumes are empty", nodeId);
            return CompletableFuture.completedFuture(new NodeResult(nodeId, Set.of(), volumeIds));
        }

        return logDirs
                .thenApply(nodeLogDirs -> {
                    Set<Integer> nonEmptyVolumes = new LinkedHashSet<>();
                    Set<Integer> uncheckedVolumes = new LinkedHashSet<>();

                    for (Integer volumeId : volumeIds) {
                        LogDirDescription logDir = nodeLogDirs.get(VolumeUtils.kafkaLogDirPath(volumeId, nodeId));

                        if (logDir == null) {
                            // The node does not have this log dir, so it holds no replicas on it
                            LOGGER.warnCr(reconciliation, "Kafka node {} does not report the log directory of volume {}", nodeId, volumeId);
                        } else if (logDir.error() != null) {
                            // An offline log dir reports an error and no replicas, so its content is not known
                            uncheckedVolumes.add(volumeId);
                        } else if (!logDir.replicaInfos().isEmpty()) {
                            nonEmptyVolumes.add(volumeId);
                        }
                    }

                    return new NodeResult(nodeId, nonEmptyVolumes, uncheckedVolumes);
                })
                .exceptionally(error -> {
                    LOGGER.warnCr(reconciliation, "Failed to get the log directories of Kafka node {}, so it is not possible to check if its volumes are empty", nodeId, error);
                    return new NodeResult(nodeId, Set.of(), volumeIds);
                });
    }

    /**
     * Creates the Admin client used to talk to the Kafka cluster
     *
     * @param reconciliation        Reconciliation marker
     * @param coIdentity            Trust set and identity for authentication for connecting to the Kafka cluster
     * @param adminClientProvider   Used to create the Admin client instance
     *
     * @return  Admin client instance
     */
    private static Admin createAdminClient(Reconciliation reconciliation, Identity coIdentity, AdminClientProvider adminClientProvider) {
        String bootstrapHostname = KafkaResources.bootstrapServiceName(reconciliation.name()) + "." + reconciliation.namespace() + ".svc:" + KafkaCluster.REPLICATION_PORT;
        LOGGER.debugCr(reconciliation, "Creating AdminClient for Kafka cluster in namespace {}", reconciliation.namespace());
        return adminClientProvider.createAdminClient(bootstrapHostname, coIdentity.trustSet(), coIdentity.authIdentity());
    }

    /**
     * This method gets the topic names after interacting with the Admin client
     *
     * @param kafkaAdmin          Instance of Kafka Admin
     * @return  a CompletionStage with set of topic names
     */
    /* test */ CompletionStage<Set<String>> topicNames(Admin kafkaAdmin) {
        return kafkaAdmin.listTopics(new ListTopicsOptions().listInternal(true)).names().toCompletionStage();
    }

    /**
     * Returns a collection of topic descriptions
     *
     * @param kafkaAdmin     Instance of Admin client
     * @param names          Set of topic names
     * @return a CompletionStage with map containing the topic name and description
     */
    /* test */ CompletionStage<Map<String, TopicDescription>> describeTopics(Admin kafkaAdmin, Set<String> names) {
        return kafkaAdmin.describeTopics(names).allTopicNames().toCompletionStage();
    }

    /**
     * Returns the log directories of the given Kafka nodes together with the partition replicas they contain. Every
     * node is returned with its own future, so that a node which does not answer does not fail the others.
     *
     * @param kafkaAdmin    Instance of Admin client
     * @param nodeIds       IDs of the Kafka nodes which should be described
     *
     * @return  Map with the node ID and a CompletionStage with its log directories
     */
    /* test */ Map<Integer, CompletionStage<Map<String, LogDirDescription>>> describeLogDirs(Admin kafkaAdmin, Set<Integer> nodeIds) {
        return kafkaAdmin.describeLogDirs(nodeIds)
                .descriptions()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().toCompletionStage()));
    }
}
