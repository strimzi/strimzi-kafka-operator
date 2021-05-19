/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Reconciliation;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    private static final Logger LOGGER = LogManager.getLogger(KafkaAvailability.class.getName());
    private static final ReconciliationLogger RECONCILIATION_LOGGER = new ReconciliationLogger(LOGGER);

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
            RECONCILIATION_LOGGER.debug(reconciliation, "Got {} topic names", names.size());
            RECONCILIATION_LOGGER.trace(reconciliation, "Topic names {}", names);
            return describeTopics(names);
        });
    }

    /**
     * Determine whether the given broker can be rolled without affecting
     * producers with acks=all publishing to topics with a {@code min.in.sync.replicas}.
     */
    Future<Boolean> canRoll(int podId) {
        RECONCILIATION_LOGGER.debug(reconciliation, "Determining whether broker {} can be rolled", podId);
        return canRollBroker(descriptions, podId);
    }

    private Future<Boolean> canRollBroker(Future<Collection<TopicDescription>> descriptions, int podId) {
        Future<Set<TopicDescription>> topicsOnGivenBroker = descriptions
                .compose(topicDescriptions -> {
                    RECONCILIATION_LOGGER.debug(reconciliation, "Got {} topic descriptions", topicDescriptions.size());
                    return Future.succeededFuture(groupTopicsByBroker(topicDescriptions, podId));
                }).recover(error -> {
                    RECONCILIATION_LOGGER.warn(reconciliation, "failed to get topic descriptions", error);
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
                RECONCILIATION_LOGGER.debug(reconciliation, "Restart pod {} would remove it from ISR, stalling producers with acks=all", podId);
            }
            return canRoll;
        }).recover(error -> {
            RECONCILIATION_LOGGER.warn(reconciliation, "Error determining whether it is safe to restart pod {}", podId, error);
            return Future.failedFuture(error);
        });
    }

    private boolean wouldAffectAvailability(int broker, Map<String, Config> nameToConfig, TopicDescription td) {
        Config config = nameToConfig.get(td.name());
        ConfigEntry minIsrConfig = config.get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG);
        int minIsr;
        if (minIsrConfig != null && minIsrConfig.value() != null) {
            minIsr = parseInt(minIsrConfig.value());
            RECONCILIATION_LOGGER.debug(reconciliation, "{} has {}={}.", td.name(), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr);
        } else {
            minIsr = -1;
            RECONCILIATION_LOGGER.debug(reconciliation, "{} lacks {}.", td.name(), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG);
        }

        for (TopicPartitionInfo pi : td.partitions()) {
            List<Node> isr = pi.isr();
            if (minIsr >= 0) {
                if (pi.replicas().size() <= minIsr) {
                    RECONCILIATION_LOGGER.debug(reconciliation, "{}/{} will be underreplicated (|ISR|={} and {}={}) if broker {} is restarted, but there are only {} replicas.",
                            td.name(), pi.partition(), isr.size(), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr, broker,
                            pi.replicas().size());
                } else if (isr.size() < minIsr
                        && contains(pi.replicas(), broker)) {
                    logIsrReplicas(td, pi, isr);
                    RECONCILIATION_LOGGER.info(reconciliation, "{}/{} is already underreplicated (|ISR|={}, {}={}); broker {} has a replica, " +
                                    "so should not be restarted right now (it might be first to catch up).",
                            td.name(), pi.partition(), isr.size(), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr, broker);
                    return true;
                } else if (isr.size() == minIsr
                        && contains(isr, broker)) {
                    if (minIsr < pi.replicas().size()) {
                        logIsrReplicas(td, pi, isr);
                        RECONCILIATION_LOGGER.info(reconciliation, "{}/{} will be underreplicated (|ISR|={} and {}={}) if broker {} is restarted.",
                                td.name(), pi.partition(), isr.size(), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr, broker);
                        return true;
                    } else {
                        RECONCILIATION_LOGGER.debug(reconciliation, "{}/{} will be underreplicated (|ISR|={} and {}={}) if broker {} is restarted, but there are only {} replicas.",
                                td.name(), pi.partition(), isr.size(), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr, broker,
                                pi.replicas().size());
                    }
                }
            }
        }
        return false;
    }

    private void logIsrReplicas(TopicDescription td, TopicPartitionInfo pi, List<Node> isr) {
        if (LOGGER.isDebugEnabled()) {
            RECONCILIATION_LOGGER.debug(reconciliation, "{}/{} has ISR={}, replicas={}", td.name(), pi.partition(), nodeList(isr), nodeList(pi.replicas()));
        }
    }

    String nodeList(List<Node> nodes) {
        return nodes.stream().map(n -> String.valueOf(n.id())).collect(Collectors.joining(",", "[", "]"));
    }

    private boolean contains(List<Node> isr, int broker) {
        return isr.stream().anyMatch(node -> node.id() == broker);
    }

    private Future<Map<String, Config>> topicConfigs(Collection<String> topicNames) {
        RECONCILIATION_LOGGER.debug(reconciliation, "Getting topic configs for {} topics", topicNames.size());
        List<ConfigResource> configs = topicNames.stream()
                .map((String topicName) -> new ConfigResource(ConfigResource.Type.TOPIC, topicName))
                .collect(Collectors.toList());
        Promise<Map<String, Config>> promise = Promise.promise();
        ac.describeConfigs(configs).all().whenComplete((topicNameToConfig, error) -> {
            if (error != null) {
                promise.fail(error);
            } else {
                RECONCILIATION_LOGGER.debug(reconciliation, "Got topic configs for {} topics", topicNames.size());
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
            RECONCILIATION_LOGGER.trace(reconciliation, td);
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
        ac.describeTopics(names).all()
                .whenComplete((tds, error) -> {
                    if (error != null) {
                        descPromise.fail(error);
                    } else {
                        RECONCILIATION_LOGGER.debug(reconciliation, "Got topic descriptions for {} topics", tds.size());
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
                        RECONCILIATION_LOGGER.debug(reconciliation, "Got {} topic names", names.size());
                        namesPromise.complete(names);
                    }
                });
        return namesPromise.future();
    }
}
