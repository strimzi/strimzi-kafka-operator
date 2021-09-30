/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
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

    private final Vertx vertx;

    private final Future<Collection<TopicDescription>> descriptions;

    KafkaAvailability(Reconciliation reconciliation, Vertx vertx, Admin ac) {
        this.vertx = vertx;
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
    Future<Boolean> canRoll(int podId, Set<Integer> restartingBrokers) {
        LOGGER.debugCr(reconciliation, "Determining whether broker {} can be rolled", podId);
        return canRollBroker(descriptions, podId, restartingBrokers);
    }

   /**
     * Return a Future which completes with the set of partitions which have the given
     * {@code broker} as their preferred leader, but which aren't currently being led by that broker.
     * @param broker The broker
     * @return a Future.
     */
    Future<Set<TopicPartition>> partitionsWithPreferredButNotCurrentLeader(int broker) {
        return descriptions.map(d -> d.stream().flatMap(td -> {
            String topic = td.name();
            return td.partitions().stream()
                    .filter(pd -> pd.replicas().size() > 0
                            && pd.replicas().get(0).id() == broker
                            && broker != pd.leader().id())
                    .map(pd -> new TopicPartition(topic, pd.partition()));
        }).collect(Collectors.toSet()));
    }


    private Future<Boolean> canRollBroker(Future<Collection<TopicDescription>> descriptions, int podId, Set<Integer> restartingBrokers) {
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
                .compose(td -> topicConfigs(td.stream().map(TopicDescription::name).collect(Collectors.toSet())));

        // 5. join
        return topicConfigsOnGivenBroker.map(topicNameToConfig -> {
            Collection<TopicDescription> tds = topicsOnGivenBroker.result();
            boolean canRoll = tds.stream().noneMatch(
                td -> wouldAffectAvailability(podId, topicNameToConfig, td, restartingBrokers));
            if (!canRoll) {
                LOGGER.debugCr(reconciliation, "Restart pod {} would remove it from ISR, stalling producers with acks=all", podId);
            }
            return canRoll;
        }).recover(error -> {
            LOGGER.warnCr(reconciliation, "Error determining whether it is safe to restart pod {}", podId, error);
            return Future.failedFuture(error);
        });
    }

    private boolean wouldAffectAvailability(int broker, Map<String, Config> nameToConfig, TopicDescription td, Set<Integer> restartingBrokers) {
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
            if (minIsr >= 0) {
                // Override the ISR given by the controller to also remove brokers which are already known to be in
                // the process of restarting (because ISR shrink and metadata propagation aren't instantaneous but we
                // might try to restart two brokers at the same time).
                List<Node> effectiveIsr = pi.isr().stream().filter(node -> !restartingBrokers.contains(node.id())).collect(Collectors.toList());
                if (pi.replicas().size() <= minIsr) {
                    LOGGER.debugCr(reconciliation, "{}/{} will be under-replicated (effective ISR={{}}, replicas=[{}], {}={}) if broker {} is restarted, but there are only {} replicas.",
                            td.name(), pi.partition(), nodeList(effectiveIsr), nodeList(pi.replicas()), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr, broker,
                            pi.replicas().size());
                } else if (effectiveIsr.size() < minIsr
                        && contains(pi.replicas(), broker)) {
                    if (LOGGER.isInfoEnabled()) {
                        String msg;
                        if (contains(effectiveIsr, broker)) {
                            msg = "{}/{} is already under-replicated (effective ISR={{}}, replicas=[{}], {}={}); broker {} is in the ISR, " +
                                    "so should not be restarted right now (it would impact consumers).";
                        } else {
                            msg = "{}/{} is already under-replicated (effective ISR={{}}, replicas=[{}], {}={}); broker {} has a replica, " +
                                    "so should not be restarted right now (it might be first to catch up).";
                        }
                        LOGGER.infoCr(reconciliation, msg,
                                td.name(), pi.partition(), nodeList(effectiveIsr), nodeList(pi.replicas()), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr, broker);
                    }
                    return true;
                } else if (effectiveIsr.size() == minIsr
                        && contains(effectiveIsr, broker)) {
                    if (minIsr < pi.replicas().size()) {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.infoCr(reconciliation, "{}/{} will be under-replicated (effective ISR={{}}, replicas=[{}], {}={}) if broker {} is restarted.",
                                    td.name(), pi.partition(), nodeList(effectiveIsr), nodeList(pi.replicas()), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr, broker);
                        }
                        return true;
                    } else {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debugCr(reconciliation, "{}/{} will be under-replicated (effective ISR={{}}, replicas=[{}], {}={}) if broker {} is restarted, but there are only {} replicas.",
                                    td.name(), pi.partition(), nodeList(effectiveIsr), nodeList(pi.replicas()), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr, broker,
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

    private static boolean contains(List<Node> isr, int broker) {
        return isr.stream().anyMatch(node -> node.id() == broker);
    }

    private Future<Map<String, Config>> topicConfigs(Collection<String> topicNames) {
        LOGGER.debugCr(reconciliation, "Getting topic configs for {} topics", topicNames.size());
        List<ConfigResource> configs = topicNames.stream()
                .map((String topicName) -> new ConfigResource(ConfigResource.Type.TOPIC, topicName))
                .collect(Collectors.toList());
        return Util.kafkaFutureToVertxFuture(reconciliation, vertx, ac.describeConfigs(configs).all().thenApply(topicNameToConfig -> {
                    LOGGER.debugCr(reconciliation, "Got topic configs for {} topics", topicNames.size());
                    return topicNameToConfig.entrySet().stream()
                            .collect(Collectors.toMap(
                                    entry -> entry.getKey().name(),
                                    Map.Entry::getValue));
                }));
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
        return Util.kafkaFutureToVertxFuture(reconciliation, vertx, ac.describeTopics(names).all().thenApply(tds -> {
            LOGGER.debugCr(reconciliation, "Got topic descriptions for {} topics", tds.size());
            return tds.values();
                }));
    }

    protected Future<Set<String>> topicNames() {
        return Util.kafkaFutureToVertxFuture(reconciliation, vertx, ac.listTopics(new ListTopicsOptions().listInternal(true)).names()
                .thenApply(names -> {
                        LOGGER.debugCr(reconciliation, "Got {} topic names", names.size());
                        return names;
                }));
    }
}
