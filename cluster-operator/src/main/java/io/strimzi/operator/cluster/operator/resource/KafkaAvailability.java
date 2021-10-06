/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;

import static java.lang.Integer.parseInt;

/**
 * Determines whether the given broker can be rolled without affecting
 * producers with acks=all publishing to topics with a {@code min.in.sync.replicas}.
 */
class KafkaAvailability {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaAvailability.class.getName());
    private final Reconciliation reconciliation;
    private final Admin admin;
    private final Vertx vertx;

    public KafkaAvailability(Reconciliation reconciliation, Vertx vertx, Admin admin) {
        this.reconciliation = reconciliation;
        this.vertx = vertx;
        this.admin = admin;
    }


    /**
     * Determine whether the given broker can be rolled without affecting
     * producers with acks=all publishing to topics with a {@code min.in.sync.replicas}.
     * If the admin client threw an exception the returned future will be failed with an AdminClientException.
     */
    public Future<Boolean> canRoll(int brokerId, Set<Integer> restartingBrokers) {
        Future<Set<String>> topicNames = topicNames();
        // 2. Get topic descriptions
        var descriptions = topicNames.compose(names -> {
            LOGGER.traceCr(reconciliation, "Topic names {}", names);
            return describeTopics(names);
        });
        LOGGER.debugCr(reconciliation, "Determining whether broker {} can be rolled", brokerId);
        Future<Set<TopicDescription>> topicsOnGivenBroker = descriptions
                .compose(topicDescriptions -> Future.succeededFuture(groupTopicsByBroker(topicDescriptions, brokerId)))
                .recover(error -> {
                    LOGGER.warnCr(reconciliation, "Failed to get topic descriptions", error);
                    return Future.failedFuture(error);
                });

        // 4. Get topic configs (for those on $broker)
        Future<Map<String, Config>> topicConfigsOnGivenBroker = topicsOnGivenBroker
                .compose(td -> topicConfigs(td.stream().map(TopicDescription::name).collect(Collectors.toSet())));

        // 5. join
        return topicConfigsOnGivenBroker.map(topicNameToConfig -> {
            Collection<TopicDescription> tds = topicsOnGivenBroker.result();
            boolean canRoll = tds.stream().noneMatch(
                td -> wouldAffectAvailability(brokerId, topicNameToConfig, td, restartingBrokers));
            if (!canRoll) {
                LOGGER.debugCr(reconciliation, "Restarting broker {} would would make at least one topic with acks=all under-replicated", brokerId);
            } else {
                LOGGER.debugCr(reconciliation, "Restarting broker {} would should not affect producers with acks=all", brokerId);
            }
            return canRoll;
        }).recover(error -> {
            LOGGER.warnCr(reconciliation, "Error determining whether it is safe to restart broker {}", brokerId, error);
            return Future.failedFuture(error);
        });
    }

   /**
    * Return a Future which completes with the set of partitions which have the given
    * {@code broker} as their preferred leader, but which aren't currently being led by that broker.
    * If the admin client threw an exception the returned future will be failed with an AdminClientException.
    * @param broker The broker
    * @return a Future.
    */
    public Future<Set<TopicPartition>> partitionsWithPreferredButNotCurrentLeader(int broker) {
        Future<Set<String>> topicNames = topicNames();
        // 2. Get topic descriptions
        var descriptions = topicNames.compose(names -> {
            LOGGER.traceCr(reconciliation, "Topic names {}", names);
            return describeTopics(names);
        });
        return descriptions.map(d -> d.stream().flatMap(td -> {
            String topic = td.name();
            return td.partitions().stream()
                    .filter(pd -> pd.replicas().size() > 0
                            && pd.replicas().get(0).id() == broker
                            && broker != pd.leader().id())
                    .map(pd -> new TopicPartition(topic, pd.partition()));
        }).collect(Collectors.toSet()));
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
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debugCr(reconciliation, "Rolling {} should not affect availability of {}",
                    broker, td.name());
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
        List<ConfigResource> configs = topicNames.stream()
                .map((String topicName) -> new ConfigResource(ConfigResource.Type.TOPIC, topicName))
                .collect(Collectors.toList());
        return Util.kafkaFutureToVertxFuture(vertx, admin.describeConfigs(configs).all())
                .recover(ex -> adminClientFailure(ex, "Admin.describeConfigs (topics)"))
                .map(topicNameToConfig -> topicNameToConfig.entrySet().stream()
                        .collect(Collectors.<Map.Entry<ConfigResource, Config>, String, Config>toMap(
                                entry -> entry.getKey().name(),
                                Map.Entry::getValue)));
    }

    private <T> Future<T> adminClientFailure(Throwable ex, String call) {
        Throwable cause;
        if (ex instanceof CompletionException) {
            cause = ex.getCause();
        } else {
            cause = ex;
        }
        return Future.failedFuture(new AdminClientException(KafkaAvailability.class.getSimpleName() + " call of " + call + " failed", cause));
    }

    private Set<TopicDescription> groupTopicsByBroker(Collection<TopicDescription> tds, int brokerId) {
        return tds.stream().filter(td ->
                td.partitions().stream().flatMap(pd ->
                        pd.replicas().stream()).anyMatch(broker ->
                        brokerId == broker.id()
                )
        ).collect(Collectors.toSet());
    }

    protected Future<Collection<TopicDescription>> describeTopics(Set<String> names) {
        return Util.kafkaFutureToVertxFuture(vertx, admin.describeTopics(names).all()
                .thenApply(Map::values))
                .recover(ex -> adminClientFailure(ex, "Admin.describeTopics"));
    }

    protected Future<Set<String>> topicNames() {
        return Util.kafkaFutureToVertxFuture(vertx, admin.listTopics(new ListTopicsOptions().listInternal(true)).names())
                .recover(ex -> adminClientFailure(ex, "Admin.listTopics"));
    }

    public Future<Map<ConfigResource.Type, Config>> brokerConfigs(int brokerId) {
        return Util.kafkaFutureToVertxFuture(
                        vertx,
                        admin.describeConfigs(
                                List.of(Util.brokerConfigResource(brokerId), Util.brokerLoggersConfigResource(brokerId))).all())
                    .map(mapOfConfigs -> mapOfConfigs.entrySet().stream()
                            .collect(Collectors.toMap(entry -> entry.getKey().type(), Map.Entry::getValue)))
                    .recover(ex -> adminClientFailure(ex, "Admin.describeConfigs (broker)"));
    }

    static class ConfigDiff {
        public final KafkaBrokerConfigurationDiff configDiff;
        public final KafkaBrokerLoggingConfigurationDiff loggersDiff;

        public ConfigDiff(KafkaBrokerConfigurationDiff configDiff, KafkaBrokerLoggingConfigurationDiff loggersDiff) {
            this.configDiff = configDiff;
            this.loggersDiff = loggersDiff;
        }
    }

    public Future<ConfigDiff> brokerConfigDiffs(int brokerId, KafkaVersion kafkaVersion, String kafkaConfig, String kafkaLogging) {
        return brokerConfigs(brokerId).map(map ->
                new ConfigDiff(new KafkaBrokerConfigurationDiff(reconciliation, map.get(ConfigResource.Type.BROKER), kafkaConfig, kafkaVersion, brokerId),
                        new KafkaBrokerLoggingConfigurationDiff(reconciliation, map.get(ConfigResource.Type.BROKER_LOGGER), kafkaLogging)));
    }

    public Future<Boolean> controller(int brokerId) {
        return Util.kafkaFutureToVertxFuture(
                        vertx,
                        admin.describeCluster(new DescribeClusterOptions().timeoutMs(5_000))
                                .controller().thenApply(controller -> controller.id() == brokerId))
                .recover(ex -> adminClientFailure(ex, "Admin.describeCluster"));
    }

    public Future<Map<TopicPartition, Optional<Throwable>>> electPreferred(Set<TopicPartition> partitions) {
        return Util.kafkaFutureToVertxFuture(
                        vertx,
                        admin.electLeaders(ElectionType.PREFERRED, partitions).partitions())
                .recover(ex -> adminClientFailure(ex, "Admin.electLeaders"));
    }

    public Future<Map<ConfigResource, Throwable>> alterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> updatedConfig) {
        AlterConfigsResult alterConfigsResult = admin.incrementalAlterConfigs(updatedConfig, new AlterConfigsOptions().timeoutMs(30_000));
        var values = alterConfigsResult.all();
        return Util.kafkaFutureToVertxFuture(vertx, values)
                .recover(ex -> adminClientFailure(ex, "Admin.incrementalAlterConfigs"))
                .map(map -> alterConfigsResult.values().entrySet().stream().collect(Collectors.toMap(
                entry -> entry.getKey(),
                entry -> {
                    KafkaFuture<Void> value = entry.getValue();
                    if (value.isCompletedExceptionally()) {
                        try {
                            value.getNow(null);
                        } catch (ExecutionException e) {
                            return e.getCause();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e); // should never happen
                        }
                    }
                    return null;
                })));
//        return values.entrySet().stream().collect(Collectors.toMap(
//                        entry -> entry.getKey(),
//                        entry -> {
//                            KafkaFuture<Void> value = entry.getValue();
//                            Future<Void> tFuture = Util.kafkaFutureToVertxFuture(vertx, value);
//                            return tFuture.recover(ex -> adminClientFailure(ex, "Admin.electLeaders"));
//                        }));
    }
}
