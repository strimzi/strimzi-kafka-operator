/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicStatusBuilder;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsHolder;
import io.strimzi.operator.topic.model.Either;
import io.strimzi.operator.topic.model.Pair;
import io.strimzi.operator.topic.model.PartitionedByError;
import io.strimzi.operator.topic.model.ReconcilableTopic;
import io.strimzi.operator.topic.model.TopicOperatorException;
import io.strimzi.operator.topic.model.TopicState;
import io.strimzi.operator.topic.model.UncheckedInterruptedException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.strimzi.operator.topic.TopicOperatorUtil.configValueAsString;
import static io.strimzi.operator.topic.TopicOperatorUtil.hasConfig;
import static io.strimzi.operator.topic.TopicOperatorUtil.partitionedByError;
import static io.strimzi.operator.topic.TopicOperatorUtil.partitions;
import static io.strimzi.operator.topic.TopicOperatorUtil.replicas;

/**
 * Handler for Kafka requests.
 */
public class KafkaHandler {
    static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaHandler.class);

    /** Default value for partitions and replicas. */
    public static final int BROKER_DEFAULT = -1;
    /** Kafka configuration for auto create topic. */
    public static final String AUTO_CREATE_TOPICS_ENABLE = "auto.create.topics.enable";
    /** Kafka configuration for min insync replicas. */
    public static final String MIN_INSYNC_REPLICAS = "min.insync.replicas";
    
    private final TopicOperatorConfig config;
    private final TopicOperatorMetricsHolder metricsHolder;
    private final Admin kafkaAdminClient;

    /**
     * Create a new instance.
     * 
     * @param config Topic Operator configuration.
     * @param metricsHolder Metrics holder.
     * @param kafkaAdminClient Kafka admin client.
     */
    KafkaHandler(TopicOperatorConfig config, TopicOperatorMetricsHolder metricsHolder, Admin kafkaAdminClient) {
        this.config = config;
        this.metricsHolder = metricsHolder;
        this.kafkaAdminClient = kafkaAdminClient;
    }

    /**
     * Retrieve the specified configuration value for a Kafka cluster.
     * <br/><br/>
     * This method queries the Kafka cluster to obtain the configuration value associated with the given name.
     * It iterates through all nodes (brokers) in the cluster, requesting their configurations, and returns the
     * value of the configuration if found. The search stops at the first occurrence of the configuration name
     * across all nodes, assuming uniform configuration across the cluster.
     *
     * @param configName The name of the configuration to retrieve.
     * @return A string containing the value of the requested configuration if found.
     * @throws RuntimeException if there is an error during the operation. 
     * This exception wraps the underlying exception's message.
     */
    public Optional<String> clusterConfig(String configName) {
        try {
            var describeClusterResult = kafkaAdminClient.describeCluster();
            var nodes = describeClusterResult.nodes().get();
            Map<ConfigResource, KafkaFuture<Map<ConfigResource, Config>>> futures = new HashMap<>();
            for (var node : nodes) {
                ConfigResource nodeResource = new ConfigResource(ConfigResource.Type.BROKER, node.idString());
                futures.put(nodeResource, kafkaAdminClient.describeConfigs(Set.of(nodeResource)).all());
            }
            for (var entry : futures.entrySet()) {
                var nodeConfig = entry.getValue().get().get(entry.getKey());
                var configEntry = nodeConfig.get(configName);
                return Optional.of(configEntry.value());
            }
            return Optional.empty();
        } catch (Throwable e) {
            throw new RuntimeException("Failed to get cluster configuration: " + e.getMessage());
        }
    }

    /**
     * Create topics.
     * 
     * @param reconcilableTopics Reconcilable topics.
     * @return Result partitioned by error.
     */
    public PartitionedByError<ReconcilableTopic, Void> createTopics(List<ReconcilableTopic> reconcilableTopics) {
        var newTopics = reconcilableTopics.stream().map(reconcilableTopic -> {
            // Admin create
            return buildNewTopic(reconcilableTopic.kt(), reconcilableTopic.topicName());
        }).collect(Collectors.toSet());

        LOGGER.debugOp("Admin.createTopics({})", newTopics);
        var timerSample = TopicOperatorUtil.startExternalRequestTimer(metricsHolder, config.enableAdditionalMetrics());
        var ctr = kafkaAdminClient.createTopics(newTopics);
        ctr.all().whenComplete((i, e) -> {
            TopicOperatorUtil.stopExternalRequestTimer(timerSample, metricsHolder::createTopicsTimer, config.enableAdditionalMetrics(), config.namespace());
            if (e != null) {
                LOGGER.traceOp("Admin.createTopics({}) failed with {}", newTopics, String.valueOf(e));
            } else {
                LOGGER.traceOp("Admin.createTopics({}) completed", newTopics);
            }
        });
        var values = ctr.values();
        return TopicOperatorUtil.partitionedByError(reconcilableTopics.stream().map(reconcilableTopic -> {
            try {
                values.get(reconcilableTopic.topicName()).get();
                reconcilableTopic.kt().setStatus(new KafkaTopicStatusBuilder()
                    .withTopicId(ctr.topicId(reconcilableTopic.topicName()).get().toString()).build());
                return new Pair<>(reconcilableTopic, Either.ofRight((null)));
            } catch (ExecutionException e) {
                if (e.getCause() != null && e.getCause() instanceof TopicExistsException) {
                    // we treat this as a success, the next reconciliation checks the configuration
                    return new Pair<>(reconcilableTopic, Either.ofRight((null)));
                } else {
                    return new Pair<>(reconcilableTopic, Either.ofLeft(handleAdminException(e)));
                }
            } catch (InterruptedException e) {
                throw new UncheckedInterruptedException(e);
            }
        }));
    }

    /**
     * Filter topics with RF change.
     * 
     * @param apparentlyDifferentRfTopics Topics with possible RF change.
     * @return Filtered list of topics with RF change.
     */
    public List<Pair<ReconcilableTopic, Either<TopicOperatorException, TopicState>>> filterByReassignmentTargetReplicas(
            List<Pair<ReconcilableTopic, TopicState>> apparentlyDifferentRfTopics) {
        if (apparentlyDifferentRfTopics.isEmpty()) {
            return List.of();
        }
        var apparentDifferentRfPartitions = apparentlyDifferentRfTopics.stream()
            .flatMap(pair -> pair.getValue().description().partitions().stream()
                .filter(pi -> {
                    // includes only the partitions of the topic with a RF that mismatches the desired RF
                    var desiredRf = pair.getKey().kt().getSpec().getReplicas();
                    return desiredRf != pi.replicas().size();
                })
                .map(pi -> new TopicPartition(pair.getKey().topicName(), pi.partition()))).collect(Collectors.toSet());
        
        Map<TopicPartition, PartitionReassignment> reassignments;
        LOGGER.traceOp("Admin.listPartitionReassignments({})", apparentDifferentRfPartitions);
        var timerSample = TopicOperatorUtil.startExternalRequestTimer(metricsHolder, config.enableAdditionalMetrics());
        try {
            reassignments = kafkaAdminClient.listPartitionReassignments(apparentDifferentRfPartitions).reassignments().get();
            TopicOperatorUtil.stopExternalRequestTimer(timerSample, metricsHolder::listReassignmentsTimer, config.enableAdditionalMetrics(), config.namespace());
            LOGGER.traceOp("Admin.listPartitionReassignments({}) completed", apparentDifferentRfPartitions);
        } catch (ExecutionException e) {
            TopicOperatorUtil.stopExternalRequestTimer(timerSample, metricsHolder::listReassignmentsTimer, config.enableAdditionalMetrics(), config.namespace());
            LOGGER.traceOp("Admin.listPartitionReassignments({}) failed with {}", apparentDifferentRfPartitions, e);
            return apparentlyDifferentRfTopics.stream().map(pair ->
                new Pair<>(pair.getKey(), Either.<TopicOperatorException, TopicState>ofLeft(handleAdminException(e)))).toList();
        } catch (InterruptedException e) {
            throw new UncheckedInterruptedException(e);
        }

        var partitionToTargetRf = reassignments.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> {
            var partitionReassignment = entry.getValue();
            // See https://cwiki.apache.org/confluence/display/KAFKA/KIP-455%3A+Create+an+Administrative+API+for+Replica+Reassignment#KIP455:CreateanAdministrativeAPIforReplicaReassignment-Algorithm
            // for a full description of the algorithm
            // but in essence replicas() will include addingReplicas() from the beginning
            // so the target rf will be the replicas minus the removing
            var target = new HashSet<>(partitionReassignment.replicas());
            partitionReassignment.removingReplicas().forEach(target::remove);
            return target.size();
        }));

        return apparentlyDifferentRfTopics.stream()
            .filter(pair -> pair.getValue().description().partitions().stream()
                .anyMatch(pi -> {
                    TopicPartition tp = new TopicPartition(pair.getKey().topicName(), pi.partition());
                    Integer targetRf = partitionToTargetRf.get(tp);
                    Integer desiredRf = pair.getKey().kt().getSpec().getReplicas();
                    return !Objects.equals(targetRf, desiredRf);
                })
            ).map(pair -> new Pair<>(pair.getKey(), Either.<TopicOperatorException, TopicState>ofRight(pair.getValue())))
            .toList();
    }

    /**
     * Alter topic configuration.
     * 
     * @param someAlterConfigs Alter configurations.
     * @return Result partitioned by error.
     */
    public PartitionedByError<ReconcilableTopic, Void> alterConfigs(List<Pair<ReconcilableTopic, Collection<AlterConfigOp>>> someAlterConfigs) {
        if (someAlterConfigs.isEmpty()) {
            return new PartitionedByError<>(List.of(), List.of());
        }
        var alteredConfigs = someAlterConfigs.stream().collect(Collectors.toMap(entry -> buildTopicConfigResource(entry.getKey().topicName()), Pair::getValue));
        LOGGER.debugOp("Admin.incrementalAlterConfigs({})", alteredConfigs);
        var timerSample = TopicOperatorUtil.startExternalRequestTimer(metricsHolder, config.enableAdditionalMetrics());
        var acr = kafkaAdminClient.incrementalAlterConfigs(alteredConfigs);
        TopicOperatorUtil.stopExternalRequestTimer(timerSample, metricsHolder::alterConfigsTimer, config.enableAdditionalMetrics(), config.namespace());
        acr.all().whenComplete((i, e) -> {
            TopicOperatorUtil.stopExternalRequestTimer(timerSample, metricsHolder::alterConfigsTimer, config.enableAdditionalMetrics(), config.namespace());
            if (e != null) {
                LOGGER.traceOp("Admin.incrementalAlterConfigs({}) failed with {}", alteredConfigs, String.valueOf(e));
            } else {
                LOGGER.traceOp("Admin.incrementalAlterConfigs({}) completed", alteredConfigs);
            }
        });
        var alterConfigsResult = acr.values();
        Stream<Pair<ReconcilableTopic, Either<TopicOperatorException, Void>>> entryStream = someAlterConfigs.stream().map(entry -> {
            try {
                return new Pair<>(entry.getKey(), Either.ofRight(alterConfigsResult.get(buildTopicConfigResource(entry.getKey().topicName())).get()));
            } catch (ExecutionException e) {
                return new Pair<>(entry.getKey(), Either.ofLeft(handleAdminException(e)));
            } catch (InterruptedException e) {
                throw new UncheckedInterruptedException(e);
            }
        });
        return TopicOperatorUtil.partitionedByError(entryStream);
    }

    /**
     * Create topic partitions.
     * 
     * @param someCreatePartitions Partitions to be created.
     * @return Result partitioned by error.
     */
    public PartitionedByError<ReconcilableTopic, Void> createPartitions(List<Pair<ReconcilableTopic, NewPartitions>> someCreatePartitions) {
        if (someCreatePartitions.isEmpty()) {
            return new PartitionedByError<>(List.of(), List.of());
        }
        var newPartitions = someCreatePartitions.stream().collect(Collectors.toMap(pair -> pair.getKey().topicName(), Pair::getValue));
        LOGGER.debugOp("Admin.createPartitions({})", newPartitions);
        var timerSample = TopicOperatorUtil.startExternalRequestTimer(metricsHolder, config.enableAdditionalMetrics());
        CreatePartitionsResult cpr = kafkaAdminClient.createPartitions(newPartitions);
        cpr.all().whenComplete((i, e) -> {
            TopicOperatorUtil.stopExternalRequestTimer(timerSample, metricsHolder::createPartitionsTimer, config.enableAdditionalMetrics(), config.namespace());
            if (e != null) {
                LOGGER.traceOp("Admin.createPartitions({}) failed with {}", newPartitions, String.valueOf(e));
            } else {
                LOGGER.traceOp("Admin.createPartitions({}) completed", newPartitions);
            }
        });
        var createPartitionsResult = cpr.values();
        var entryStream = someCreatePartitions.stream().map(entry -> {
            try {
                createPartitionsResult.get(entry.getKey().topicName()).get();
                return new Pair<>(entry.getKey(), Either.<TopicOperatorException, Void>ofRight(null));
            } catch (ExecutionException e) {
                return new Pair<>(entry.getKey(), Either.<TopicOperatorException, Void>ofLeft(handleAdminException(e)));
            } catch (InterruptedException e) {
                throw new UncheckedInterruptedException(e);
            }
        });
        return TopicOperatorUtil.partitionedByError(entryStream);
    }

    /**
     * Describe topics.
     * 
     * @param reconcilableTopics Topics to describe.
     * @return Result partitioned by error.
     */
    public PartitionedByError<ReconcilableTopic, TopicState> describeTopics(List<ReconcilableTopic> reconcilableTopics) {
        if (reconcilableTopics.isEmpty()) {
            return new PartitionedByError<>(List.of(), List.of());
        }
        Set<ConfigResource> configResources = reconcilableTopics.stream()
            .map(reconcilableTopic -> buildTopicConfigResource(reconcilableTopic.topicName()))
            .collect(Collectors.toSet());
        Set<String> tns = reconcilableTopics.stream().map(ReconcilableTopic::topicName).collect(Collectors.toSet());

        DescribeTopicsResult describeTopicsResult;
        {
            LOGGER.debugOp("Admin.describeTopics({})", tns);
            var timerSample = TopicOperatorUtil.startExternalRequestTimer(metricsHolder, config.enableAdditionalMetrics());
            describeTopicsResult = kafkaAdminClient.describeTopics(tns);
            describeTopicsResult.allTopicNames().whenComplete((i, e) -> {
                TopicOperatorUtil.stopExternalRequestTimer(timerSample, metricsHolder::describeTopicsTimer, config.enableAdditionalMetrics(), config.namespace());
                if (e != null) {
                    LOGGER.traceOp("Admin.describeTopics({}) failed with {}", tns, String.valueOf(e));
                } else {
                    LOGGER.traceOp("Admin.describeTopics({}) completed", tns);
                }
            });
        }
        DescribeConfigsResult describeConfigsResult;
        {
            LOGGER.debugOp("Admin.describeConfigs({})", configResources);
            var timerSample = TopicOperatorUtil.startExternalRequestTimer(metricsHolder, config.enableAdditionalMetrics());
            describeConfigsResult = kafkaAdminClient.describeConfigs(configResources);
            describeConfigsResult.all().whenComplete((i, e) -> {
                TopicOperatorUtil.stopExternalRequestTimer(timerSample, metricsHolder::describeConfigsTimer, config.enableAdditionalMetrics(), config.namespace());
                if (e != null) {
                    LOGGER.traceOp("Admin.describeConfigs({}) failed with {}", configResources, String.valueOf(e));
                } else {
                    LOGGER.traceOp("Admin.describeConfigs({}) completed", configResources);
                }
            });
        }

        var cs1 = describeTopicsResult.topicNameValues();
        var cs2 = describeConfigsResult.values();
        return partitionedByError(reconcilableTopics.stream().map(reconcilableTopic -> {
            Config configs = null;
            TopicDescription description = null;
            ExecutionException exception = null;
            try {
                description = cs1.get(reconcilableTopic.topicName()).get();
            } catch (ExecutionException e) {
                exception = e;
            } catch (InterruptedException e) {
                throw new UncheckedInterruptedException(e);
            }

            try {
                configs = cs2.get(buildTopicConfigResource(reconcilableTopic.topicName())).get();
            } catch (ExecutionException e) {
                exception = e;
            } catch (InterruptedException e) {
                throw new UncheckedInterruptedException(e);
            }
            if (exception != null) {
                return new Pair<>(reconcilableTopic, Either.ofLeft(handleAdminException(exception)));
            } else {
                return new Pair<>(reconcilableTopic, Either.ofRight(new TopicState(description, configs)));
            }
        }));
    }

    /**
     * Delete topics.
     * 
     * @param reconcilableTopics List of topics.
     * @param topicNames Topic names to delete.
     * @return Result partitioned by error.
     */
    public PartitionedByError<ReconcilableTopic, Object> deleteTopics(List<ReconcilableTopic> reconcilableTopics, Set<String> topicNames) {
        if (topicNames.isEmpty()) {
            return new PartitionedByError<>(List.of(), List.of());
        }
        var someDeleteTopics = TopicCollection.ofTopicNames(topicNames);
        LOGGER.debugOp("Admin.deleteTopics({})", someDeleteTopics.topicNames());

        // Admin delete
        var timerSample = TopicOperatorUtil.startExternalRequestTimer(metricsHolder, config.enableAdditionalMetrics());
        var dtr = kafkaAdminClient.deleteTopics(someDeleteTopics);
        dtr.all().whenComplete((i, e) -> {
            TopicOperatorUtil.stopExternalRequestTimer(timerSample, metricsHolder::deleteTopicsTimer, config.enableAdditionalMetrics(), config.namespace());
            if (e != null) {
                LOGGER.traceOp("Admin.deleteTopics({}) failed with {}", someDeleteTopics.topicNames(), String.valueOf(e));
            } else {
                LOGGER.traceOp("Admin.deleteTopics({}) completed", someDeleteTopics.topicNames());
            }
        });
        var futuresMap = dtr.topicNameValues();
        
        return TopicOperatorUtil.partitionedByError(reconcilableTopics.stream()
            .filter(reconcilableTopic -> futuresMap.get(reconcilableTopic.topicName()) != null)
            .map(reconcilableTopic -> {
                try {
                    futuresMap.get(reconcilableTopic.topicName()).get();
                    return new Pair<>(reconcilableTopic, Either.ofRight(null));
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                        return new Pair<>(reconcilableTopic, Either.ofRight(null));
                    } else {
                        return new Pair<>(reconcilableTopic, Either.ofLeft(handleAdminException(e)));
                    }
                } catch (InterruptedException e) {
                    throw new UncheckedInterruptedException(e);
                }
            }));
    }
    
    private static NewTopic buildNewTopic(KafkaTopic kafkaTopic, String topicName) {
        return new NewTopic(topicName, partitions(kafkaTopic), replicas(kafkaTopic)).configs(buildConfigsMap(kafkaTopic));
    }

    private static Map<String, String> buildConfigsMap(KafkaTopic kafkaTopic) {
        Map<String, String> configs = new HashMap<>();
        if (hasConfig(kafkaTopic)) {
            for (var entry : kafkaTopic.getSpec().getConfig().entrySet()) {
                configs.put(entry.getKey(), configValueAsString(entry.getValue()));
            }
        }
        return configs;
    }

    private static ConfigResource buildTopicConfigResource(String topicName) {
        return new ConfigResource(ConfigResource.Type.TOPIC, topicName);
    }

    private static TopicOperatorException handleAdminException(ExecutionException e) {
        var cause = e.getCause();
        if (cause instanceof ApiException) {
            return new TopicOperatorException.KafkaError((ApiException) cause);
        } else {
            return new TopicOperatorException.InternalError(cause);
        }
    }
}
