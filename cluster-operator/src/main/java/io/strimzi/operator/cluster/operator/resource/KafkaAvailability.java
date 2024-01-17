/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.vertx.core.Future;
import io.vertx.core.Promise;
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
import java.util.stream.Collectors;

import static java.lang.Integer.parseInt;

/**
 * Determines whether the given broker can be rolled without affecting
 * producers with acks=all publishing to topics with a {@code min.in.sync.replicas}.
 */
class KafkaAvailability {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaAvailability.class.getName());

    private final Admin ac;

    private final Reconciliation reconciliation;

    private final Future<Collection<TopicDescription>> descriptions;

    KafkaAvailability(Reconciliation reconciliation, Admin ac) {
        this.ac = ac;
        this.reconciliation = reconciliation;
        // 1. Get all topic names
        Future<Set<String>> topicNames = topicNames();
        // 2. Get topic descriptions
        descriptions = topicNames.compose(names -> {
            LOGGER.debugCr(reconciliation, "Got {} topic names", names.size());
            LOGGER.traceCr(reconciliation, "Topic names {}", names);
            return describeTopics(names);
        });
    }

    /**
     * Determine whether the given broker can be rolled without affecting
     * producers with acks=all publishing to topics with a {@code min.in.sync.replicas}.
     */
    Future<Boolean> canRoll(int podId) {
        LOGGER.debugCr(reconciliation, "Determining whether broker {} can be rolled", podId);
        return canRollBroker(descriptions, podId);
    }

    private Future<Boolean> canRollBroker(Future<Collection<TopicDescription>> descriptions, int podId) {
        Future<Set<TopicDescription>> topicsOnGivenBroker = descriptions
                .compose(topicDescriptions -> {
                    LOGGER.debugCr(reconciliation, "Got {} topic descriptions", topicDescriptions.size());
                    return Future.succeededFuture(groupTopicsByBroker(topicDescriptions, podId));
                }).recover(error -> {
                    LOGGER.warnCr(reconciliation, "failed to get topic descriptions", error);
                    return Future.failedFuture(error);
                });

        // 4. Get topic configs (for those on $broker)
        Future<Map<String, Config>> topicConfigsOnGivenBroker = topicsOnGivenBroker
                .compose(td -> topicConfigs(td.stream().map(t -> t.name()).collect(Collectors.toSet())));

        // 5. join
        return topicConfigsOnGivenBroker.map(topicNameToConfig -> {
            Collection<TopicDescription> tds = topicsOnGivenBroker.result();
            boolean canRoll = tds.stream().noneMatch(
                td -> wouldAffectAvailability(podId, topicNameToConfig, td));
            if (!canRoll) {
                LOGGER.debugCr(reconciliation, "Restart pod {} would remove it from ISR, stalling producers with acks=all", podId);
            }
            return canRoll;
        }).recover(error -> {
            LOGGER.warnCr(reconciliation, "Error determining whether it is safe to restart pod {}", podId, error);
            return Future.failedFuture(error);
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

    private Future<Map<String, Config>> topicConfigs(Collection<String> topicNames) {
        LOGGER.debugCr(reconciliation, "Getting topic configs for {} topics", topicNames.size());
        List<ConfigResource> configs = topicNames.stream()
                .map((String topicName) -> new ConfigResource(ConfigResource.Type.TOPIC, topicName))
                .collect(Collectors.toList());
        Promise<Map<String, Config>> promise = Promise.promise();
        ac.describeConfigs(configs).all().whenComplete((topicNameToConfig, error) -> {
            if (error != null) {
                promise.fail(error);
            } else {
                LOGGER.debugCr(reconciliation, "Got topic configs for {} topics", topicNames.size());
                promise.complete(topicNameToConfig.entrySet().stream()
                        .collect(Collectors.toMap(
                            entry -> entry.getKey().name(),
                            entry -> entry.getValue())));
            }
        });
        return promise.future();
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

    protected Future<Collection<TopicDescription>> describeTopics(Set<String> names) {
        Promise<Collection<TopicDescription>> descPromise = Promise.promise();
        ac.describeTopics(names).allTopicNames()
                .whenComplete((tds, error) -> {
                    if (error != null) {
                        descPromise.fail(error);
                    } else {
                        LOGGER.debugCr(reconciliation, "Got topic descriptions for {} topics", tds.size());
                        descPromise.complete(tds.values());
                    }
                });
        return descPromise.future();
    }

    protected Future<Set<String>> topicNames() {
        Promise<Set<String>> namesPromise = Promise.promise();
        ac.listTopics(new ListTopicsOptions().listInternal(true)).names()
                .whenComplete((names, error) -> {
                    if (error != null) {
                        namesPromise.fail(error);
                    } else {
                        LOGGER.debugCr(reconciliation, "Got {} topic names", names.size());
                        namesPromise.complete(names);
                    }
                });
        return namesPromise.future();
    }
}
