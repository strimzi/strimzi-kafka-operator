/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.vertx.core.Future;
import org.apache.kafka.clients.admin.AdminClient;
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

    private static final Logger log = LogManager.getLogger(KafkaAvailability.class.getName());

    private final AdminClient ac;
    private final Future<Collection<TopicDescription>> descriptions;

    KafkaAvailability(AdminClient ac) {
        this.ac = ac;
        // 1. Get all topic names
        Future<Set<String>> topicNames = topicNames();
        // 2. Get topic descriptions
        descriptions = topicNames.compose(names -> {
            log.debug("Got {} topic names", names.size());
            log.trace("Topic names {}", names);
            return describeTopics(names);
        });
    }

    /**
     * Determine whether the given broker can be rolled without affecting
     * producers with acks=all publishing to topics with a {@code min.in.sync.replicas}.
     */
    Future<Boolean> canRoll(int podId) {
        log.debug("Determining whether broker {} can be rolled", podId);
        return canRollBroker(descriptions, podId);
    }

    private Future<Boolean> canRollBroker(Future<Collection<TopicDescription>> descriptions, int podId) {
        Future<Set<TopicDescription>> topicsOnGivenBroker = descriptions
                .compose(topicDescriptions -> {
                    log.debug("Got {} topic descriptions", topicDescriptions.size());
                    return Future.succeededFuture(groupTopicsByBroker(topicDescriptions, podId));
                }).recover(error -> {
                    log.warn(error);
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
                log.debug("Restart pod {} would remove it from ISR, stalling producers with acks=all", podId);
            }
            return canRoll;
        }).recover(error -> {
            log.warn("Error determining whether it is safe to restart pod {}", podId, error);
            return Future.failedFuture(error);
        });
    }

    private boolean wouldAffectAvailability(int broker, Map<String, Config> nameToConfig, TopicDescription td) {
        Config config = nameToConfig.get(td.name());
        ConfigEntry minIsrConfig = config.get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG);
        int minIsr;
        if (minIsrConfig != null && minIsrConfig.value() != null) {
            minIsr = parseInt(minIsrConfig.value());
            log.debug("{} has {}={}.", td.name(), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr);
        } else {
            minIsr = -1;
            log.debug("{} lacks {}.", td.name(), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG);
        }

        for (TopicPartitionInfo pi : td.partitions()) {
            List<Node> isr = pi.isr();
            if (minIsr >= 0) {
                if (isr.size() < minIsr
                        && contains(pi.replicas(), broker)) {
                    logIsrReplicas(td, pi, isr);
                    log.info("{}/{} is already underreplicated (|ISR|={}, {}={}); broker {} has a replica, " +
                                    "so should not be restarted right now (it might be first to catch up).",
                            td.name(), pi.partition(), isr.size(), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr, broker);
                    return true;
                } else if (isr.size() == minIsr
                        && contains(isr, broker)) {
                    if (minIsr < pi.replicas().size()) {
                        logIsrReplicas(td, pi, isr);
                        log.info("{}/{} will be underreplicated (|ISR|={} and {}={}) if broker {} is restarted.",
                                td.name(), pi.partition(), isr.size(), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr, broker);
                        return true;
                    } else {
                        log.debug("{}/{} will be underreplicated (|ISR|={} and {}={}) if broker {} is restarted, but there are only {} relicas.",
                                td.name(), pi.partition(), isr.size(), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr, broker,
                                pi.replicas().size());
                    }
                }
            }
        }
        return false;
    }

    private void logIsrReplicas(TopicDescription td, TopicPartitionInfo pi, List<Node> isr) {
        if (log.isDebugEnabled()) {
            log.debug("{}/{} has ISR={}, replicas={}", td.name(), pi.partition(), nodeList(isr), nodeList(pi.replicas()));
        }
    }

    String nodeList(List<Node> nodes) {
        return nodes.stream().map(n -> String.valueOf(n.id())).collect(Collectors.joining(",", "[", "]"));
    }

    private boolean contains(List<Node> isr, int broker) {
        return isr.stream().anyMatch(node -> node.id() == broker);
    }

    private Future<Map<String, Config>> topicConfigs(Collection<String> topicNames) {
        log.debug("Getting topic configs for {} topics", topicNames.size());
        List<ConfigResource> configs = topicNames.stream()
                .map((String topicName) -> new ConfigResource(ConfigResource.Type.TOPIC, topicName))
                .collect(Collectors.toList());
        Future<Map<String, Config>> f = Future.future();
        ac.describeConfigs(configs).all().whenComplete((topicNameToConfig, error) -> {
            if (error != null) {
                f.fail(error);
            } else {
                log.debug("Got topic configs for {} topics", topicNames.size());
                f.complete(topicNameToConfig.entrySet().stream()
                        .collect(Collectors.toMap(
                            entry -> entry.getKey().name(),
                            entry -> entry.getValue())));
            }
        });
        return f;
    }

    private Set<TopicDescription> groupTopicsByBroker(Collection<TopicDescription> tds, int podId) {
        Set<TopicDescription> topicPartitionInfos = new HashSet<>();
        for (TopicDescription td : tds) {
            log.trace("{}", td);
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
        Future<Collection<TopicDescription>> descFuture = Future.future();
        ac.describeTopics(names).all()
                .whenComplete((tds, error) -> {
                    if (error != null) {
                        descFuture.fail(error);
                    } else {
                        log.debug("Got topic descriptions for {} topics", tds.size());
                        descFuture.complete(tds.values());
                    }
                });
        return descFuture;
    }

    protected Future<Set<String>> topicNames() {
        Future<Set<String>> namesFuture = Future.future();
        ac.listTopics(new ListTopicsOptions().listInternal(true)).names()
                .whenComplete((names, error) -> {
                    if (error != null) {
                        namesFuture.fail(error);
                    } else {
                        log.debug("Got {} topic names", names.size());
                        namesFuture.complete(names);
                    }
                });
        return namesFuture;
    }
}
