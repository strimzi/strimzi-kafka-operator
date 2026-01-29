/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.strimzi.operator.common.Util.unwrap;
import static java.lang.Integer.parseInt;

/**
 * Determines whether the given broker can be rolled without affecting
 * producers with acks=all publishing to topics with a {@code min.in.sync.replicas}.
 */
class KafkaAvailability {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaAvailability.class.getName());

    private final Admin ac;

    private final Reconciliation reconciliation;

    private final CompletableFuture<Collection<TopicDescription>> descriptions;

    KafkaAvailability(Reconciliation reconciliation, Admin ac) {
        this.ac = ac;
        this.reconciliation = reconciliation;
        // 1. Get all topic names
        CompletableFuture<Set<String>> topicNames = topicNames();
        // 2. Get topic descriptions
        descriptions = topicNames.thenCompose(names -> {
            LOGGER.debugCr(reconciliation, "Got {} topic names", names.size());
            LOGGER.traceCr(reconciliation, "Topic names {}", names);
            return describeTopics(names);
        });
    }

    /**
     * Determine whether the given broker can be rolled without affecting
     * producers with acks=all publishing to topics with a {@code min.in.sync.replicas}.
     */
    CompletableFuture<Boolean> canRoll(int podId) {
        LOGGER.debugCr(reconciliation, "Determining whether broker {} can be rolled", podId);
        return canRollBroker(descriptions, podId);
    }

    private CompletableFuture<Boolean> canRollBroker(CompletableFuture<Collection<TopicDescription>> descriptions, int podId) {
        CompletableFuture<Set<TopicDescription>> topicsOnGivenBroker = descriptions
                .whenComplete((r, error) -> {
                    if (error != null) {
                        Throwable cause = unwrap(error);
                        LOGGER.warnCr(reconciliation, "failed to get topic descriptions", cause);
                    }
                }).thenApply(topicDescriptions -> {
                    LOGGER.debugCr(reconciliation, "Got {} topic descriptions", topicDescriptions.size());
                    return groupTopicsByBroker(topicDescriptions, podId);
                });

        // 4. Get topic configs (for those on $broker)
        CompletableFuture<Map<String, Config>> topicConfigsOnGivenBroker = topicsOnGivenBroker
                .thenCompose(td -> topicConfigs(td.stream().map(t -> t.name()).collect(Collectors.toSet())));

        // 5. join
        return topicsOnGivenBroker.thenCombine(topicConfigsOnGivenBroker, (tds, topicNameToConfig) -> {
            boolean canRoll = tds.stream().noneMatch(
                td -> wouldAffectAvailability(podId, topicNameToConfig, td));
            if (!canRoll) {
                LOGGER.debugCr(reconciliation, "Restart pod {} would remove it from ISR, stalling producers with acks=all", podId);
            }
            return canRoll;
        }).whenComplete((r, error) -> {
            if (error != null) {
                Throwable cause = unwrap(error);
                LOGGER.warnCr(reconciliation, "Error determining whether it is safe to restart pod {}", podId, cause);
            }
        });
    }

    private boolean wouldAffectAvailability(int broker, Map<String, Config> nameToConfig, TopicDescription td) {
        Config config = nameToConfig.get(td.name());
        ConfigEntry minIsrConfig = config.get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG);
        int minIsr;
        if (minIsrConfig != null && minIsrConfig.value() != null) {
            minIsr = parseInt(minIsrConfig.value());
            LOGGER.debugCr(reconciliation, "{} has {}={}.", td.name(), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr);
        } else {
            minIsr = -1;
            LOGGER.debugCr(reconciliation, "{} lacks {}.", td.name(), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG);
        }

        for (TopicPartitionInfo pi : td.partitions()) {
            List<Node> isr = pi.isr();
            if (minIsr >= 0) {
                if (pi.replicas().size() <= minIsr) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debugCr(reconciliation, "{}/{} will be under-replicated (ISR={{}}, replicas=[{}], {}={}) if broker {} is restarted, but there are only {} replicas.",
                                td.name(), pi.partition(), nodeList(isr), nodeList(pi.replicas()), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr, broker,
                                pi.replicas().size());
                    }
                } else if (isr.size() < minIsr
                        && contains(pi.replicas(), broker)) {
                    if (LOGGER.isInfoEnabled()) {
                        String msg;
                        if (contains(isr, broker)) {
                            msg = "{}/{} is already under-replicated (ISR={{}}, replicas=[{}], {}={}); broker {} is in the ISR, " +
                                                          "so should not be restarted right now (it would impact consumers).";
                        } else {
                            msg = "{}/{} is already under-replicated (ISR={{}}, replicas=[{}], {}={}); broker {} has a replica, " +
                                                          "so should not be restarted right now (it might be first to catch up).";
                        }
                        LOGGER.infoCr(reconciliation, msg,
                                td.name(), pi.partition(), nodeList(isr), nodeList(pi.replicas()), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr, broker);
                    }
                    return true;
                } else if (isr.size() == minIsr
                        && contains(isr, broker)) {
                    if (minIsr < pi.replicas().size()) {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.infoCr(reconciliation, "{}/{} will be under-replicated (ISR={{}}, replicas=[{}], {}={}) if broker {} is restarted.",
                                    td.name(), pi.partition(), nodeList(isr), nodeList(pi.replicas()), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr, broker);
                        }
                        return true;
                    } else {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debugCr(reconciliation, "{}/{} will be under-replicated (ISR={{}}, replicas=[{}], {}={}) if broker {} is restarted, but there are only {} replicas.",
                                    td.name(), pi.partition(), nodeList(isr), nodeList(pi.replicas()), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr, broker,
                                    pi.replicas().size());
                        }
                    }
                }
            }
        }
        return false;
    }

    private String nodeList(List<Node> isr) {
        return isr.stream().map(Node::idString).collect(Collectors.joining(","));
    }

    private boolean contains(List<Node> isr, int broker) {
        return isr.stream().anyMatch(node -> node.id() == broker);
    }

    private CompletableFuture<Map<String, Config>> topicConfigs(Collection<String> topicNames) {
        LOGGER.debugCr(reconciliation, "Getting topic configs for {} topics", topicNames.size());
        List<ConfigResource> configs = topicNames.stream()
                .map((String topicName) -> new ConfigResource(ConfigResource.Type.TOPIC, topicName))
                .collect(Collectors.toList());
        CompletableFuture<Map<String, Config>> result = new CompletableFuture<>();
        ac.describeConfigs(configs).all().whenComplete((topicNameToConfig, error) -> {
            if (error != null) {
                result.completeExceptionally(unwrap(error));
            } else {
                LOGGER.debugCr(reconciliation, "Got topic configs for {} topics", topicNames.size());
                result.complete(topicNameToConfig.entrySet().stream()
                        .collect(Collectors.toMap(
                            entry -> entry.getKey().name(),
                            entry -> entry.getValue())));
            }
        });
        return result;
    }

    private Set<TopicDescription> groupTopicsByBroker(Collection<TopicDescription> tds, int podId) {
        Set<TopicDescription> topicPartitionInfos = new HashSet<>();
        for (TopicDescription td : tds) {
            LOGGER.traceCr(reconciliation, td);
            for (TopicPartitionInfo pd : td.partitions()) {
                for (Node broker : pd.replicas()) {
                    if (podId == broker.id()) {
                        topicPartitionInfos.add(td);
                    }
                }
            }
        }
        return topicPartitionInfos;
    }

    protected CompletableFuture<Collection<TopicDescription>> describeTopics(Set<String> names) {
        CompletableFuture<Collection<TopicDescription>> descFuture = new CompletableFuture<>();
        ac.describeTopics(names).allTopicNames()
                .whenComplete((tds, error) -> {
                    if (error != null) {
                        descFuture.completeExceptionally(unwrap(error));
                    } else {
                        LOGGER.debugCr(reconciliation, "Got topic descriptions for {} topics", tds.size());
                        descFuture.complete(tds.values());
                    }
                });
        return descFuture;
    }

    protected CompletableFuture<Set<String>> topicNames() {
        CompletableFuture<Set<String>> namesFuture = new CompletableFuture<>();
        ac.listTopics(new ListTopicsOptions().listInternal(true)).names()
                .whenComplete((names, error) -> {
                    if (error != null) {
                        namesFuture.completeExceptionally(unwrap(error));
                    } else {
                        LOGGER.debugCr(reconciliation, "Got {} topic names", names.size());
                        namesFuture.complete(names);
                    }
                });
        return namesFuture;
    }
}
