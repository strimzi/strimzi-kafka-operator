/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.common.ConditionBuilder;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicStatusBuilder;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.StatusUtils;
import io.strimzi.operator.topic.cruisecontrol.CruiseControlHandler;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsHolder;
import io.strimzi.operator.topic.model.Either;
import io.strimzi.operator.topic.model.KubeRef;
import io.strimzi.operator.topic.model.Pair;
import io.strimzi.operator.topic.model.PartitionedByError;
import io.strimzi.operator.topic.model.ReconcilableTopic;
import io.strimzi.operator.topic.model.Results;
import io.strimzi.operator.topic.model.TopicOperatorException;
import io.strimzi.operator.topic.model.TopicState;
import io.strimzi.operator.topic.model.UncheckedInterruptedException;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.common.errors.TopicDeletionDisabledException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.io.InterruptedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.strimzi.api.kafka.model.topic.ReplicasChangeState.ONGOING;
import static io.strimzi.api.kafka.model.topic.ReplicasChangeState.PENDING;
import static io.strimzi.operator.topic.TopicOperatorUtil.hasConfig;
import static io.strimzi.operator.topic.TopicOperatorUtil.isPaused;
import static io.strimzi.operator.topic.TopicOperatorUtil.partitionedByError;
import static io.strimzi.operator.topic.TopicOperatorUtil.startReconciliationTimer;
import static io.strimzi.operator.topic.TopicOperatorUtil.stopReconciliationTimer;
import static io.strimzi.operator.topic.TopicOperatorUtil.topicNames;
import static java.lang.String.format;
import static java.util.function.UnaryOperator.identity;

/**
 * Controller that reconciles batches of {@link KafkaTopic} events.
 */
@SuppressWarnings({"checkstyle:ClassFanOutComplexity", "checkstyle:ClassDataAbstractionCoupling"})
public class BatchingTopicController {
    static final ReconciliationLogger LOGGER = ReconciliationLogger.create(BatchingTopicController.class);
    static final List<String> THROTTLING_CONFIG = List.of("follower.replication.throttled.replicas", "leader.replication.throttled.replicas");

    private final TopicOperatorConfig config;
    private final Map<String, String> selector;

    private final KubernetesHandler kubernetesHandler;
    private final KafkaHandler kafkaHandler;
    private final TopicOperatorMetricsHolder metricsHolder;
    private final CruiseControlHandler cruiseControlHandler;

    // key: topic name, value: The KafkaTopics known to manage that topic
    /* test */ final Map<String, List<KubeRef>> topics;
    private final Set<String> alterableConfigs;

    BatchingTopicController(TopicOperatorConfig config,
                            Map<String, String> selector,
                            KubernetesHandler kubernetesHandler,
                            KafkaHandler kafkaHandler,
                            TopicOperatorMetricsHolder metricsHolder,
                            CruiseControlHandler cruiseControlHandler) {
        this.config = config;
        this.selector = Objects.requireNonNull(selector);
        this.kubernetesHandler = kubernetesHandler;
        this.kafkaHandler = kafkaHandler;

        var skipClusterConfigReview = config.skipClusterConfigReview();
        if (!skipClusterConfigReview) {
            // Get the config of some broker and check whether auto topic creation is enabled
            Optional<String> autoCreateValue = kafkaHandler.clusterConfig(KafkaHandler.AUTO_CREATE_TOPICS_ENABLE);
            if (autoCreateValue.filter("true"::equals).isPresent()) {
                LOGGER.warnOp(
                    "It is recommended that " + KafkaHandler.AUTO_CREATE_TOPICS_ENABLE + " is set to 'false' " +
                        "to avoid races between the operator and Kafka applications auto-creating topics");
            }
        }

        this.metricsHolder = metricsHolder;
        this.cruiseControlHandler = cruiseControlHandler;

        this.topics = new HashMap<>();
        
        var alterableConfigString = config.alterableTopicConfig();
        if (alterableConfigString == null
                || alterableConfigString.equalsIgnoreCase("ALL")
                || alterableConfigString.isEmpty()) {
            this.alterableConfigs = null;
        } else if (alterableConfigString.equalsIgnoreCase("NONE")) {
            this.alterableConfigs = Set.of();
        } else {
            this.alterableConfigs = Arrays.stream(alterableConfigString.replaceAll("\\s", "").split(",")).collect(Collectors.toSet());
        }
    }

    /**
     * @param reconcilableTopics The topics to reconcile.
     * @throws InterruptedException If the thread was interrupted while blocking.
     */
    void onDelete(List<ReconcilableTopic> reconcilableTopics) throws InterruptedException {
        try {
            deleteInternal(reconcilableTopics, true);
        } catch (UncheckedInterruptedException e) {
            throw e.getCause();
        } catch (KubernetesClientException e) {
            if (e.getCause() instanceof InterruptedIOException) {
                throw new InterruptedException();
            } else {
                throw e;
            }
        }
    }

    private void deleteInternal(List<ReconcilableTopic> reconcilableTopics, boolean onDeletePath) {
        metricsHolder.reconciliationsCounter(config.namespace()).increment(reconcilableTopics.size());
        var managedToDelete = reconcilableTopics.stream().filter(reconcilableTopic -> {
            if (TopicOperatorUtil.isManaged(reconcilableTopic.kt())) {
                var e = validate(reconcilableTopic);
                if (e.isRightEqual(true)) {
                    // adminDelete, removeFinalizer, forgetTopic, updateStatus
                    return true;
                } else if (e.isRightEqual(false)) {
                    // do nothing
                    return false;
                } else {
                    updateStatusForException(reconcilableTopic, e.left());
                    return false;
                }
            } else {
                deleteUnmanagedTopic(reconcilableTopic);
                return false;
            }
        });

        deleteManagedTopics(reconcilableTopics, onDeletePath, managedToDelete);
    }

    private void deleteUnmanagedTopic(ReconcilableTopic reconcilableTopic) {
        var timerSample = startReconciliationTimer(metricsHolder);
        kubernetesHandler.removeFinalizer(reconcilableTopic);
        forgetReconcilableTopic(reconcilableTopic);
        TopicOperatorUtil.stopReconciliationTimer(metricsHolder, timerSample, config.namespace());
        metricsHolder.successfulReconciliationsCounter(config.namespace()).increment();
    }

    private void deleteManagedTopics(List<ReconcilableTopic> reconcilableTopics, boolean onDeletePath, Stream<ReconcilableTopic> managedToDelete) {
        var timerSamples = reconcilableTopics.stream().collect(
            Collectors.toMap(identity(), rt -> startReconciliationTimer(metricsHolder)));

        var topicNames = managedToDelete.map(ReconcilableTopic::topicName).collect(Collectors.toSet());
        var deleteResult = kafkaHandler.deleteTopics(reconcilableTopics, topicNames);

        // remove the finalizer and forget the topic
        deleteResult.ok().forEach(pair -> {
            try {
                kubernetesHandler.removeFinalizer(pair.getKey());
            } catch (KubernetesClientException e) {
                // If this method be being called because the resource was deleted
                // then we expect the PATCH will error with Not Found
                if (!(onDeletePath && e.getCode() == 404)) { // 404 = Not Found
                    throw e;
                }
            }
            forgetReconcilableTopic(pair.getKey());
            metricsHolder.successfulReconciliationsCounter(config.namespace()).increment();
        });

        // join that to fail
        deleteResult.errors().forEach(entry -> {
            if (!config.useFinalizer() && onDeletePath) {
                // When not using finalizers and a topic is deleted there will be no KafkaTopic to update
                // the status of, so we have to log errors here.
                if (entry.getValue().getCause() instanceof TopicDeletionDisabledException) {
                    LOGGER.warnCr(entry.getKey().reconciliation(),
                        "Unable to delete topic '{}' from Kafka because topic deletion is disabled on the Kafka controller.",
                        entry.getKey().topicName());
                } else {
                    LOGGER.warnCr(entry.getKey().reconciliation(),
                        "Unable to delete topic '{}' from Kafka.",
                        entry.getKey().topicName(),
                        entry.getValue());
                }
                metricsHolder.failedReconciliationsCounter(config.namespace()).increment();
            } else {
                updateStatusForException(entry.getKey(), entry.getValue());
            }
        });
        timerSamples.keySet().forEach(rt -> stopReconciliationTimer(metricsHolder, timerSamples.get(rt), config.namespace()));
    }

    /**
     * @param reconcilableTopics The topics to reconcile.
     * @throws InterruptedException If the thread was interrupted while blocking.
     */
    void onUpdate(List<ReconcilableTopic> reconcilableTopics) throws InterruptedException {
        try {
            updateInternal(reconcilableTopics);
        } catch (UncheckedInterruptedException e) {
            throw e.getCause();
        } catch (KubernetesClientException e) {
            if (e.getCause() instanceof InterruptedIOException) {
                throw new InterruptedException();
            } else {
                throw e;
            }
        }
    }

    private void updateInternal(List<ReconcilableTopic> reconcilableTopics) {
        LOGGER.debugOp("Reconciling batch {}", reconcilableTopics);
        // process deletions
        var partitionedByDeletion = reconcilableTopics.stream().filter(reconcilableTopic -> {
            var kt = reconcilableTopic.kt();
            if (!matchesSelector(selector, kt.getMetadata().getLabels())) {
                forgetReconcilableTopic(reconcilableTopic);
                LOGGER.debugCr(reconcilableTopic.reconciliation(), "Ignoring KafkaTopic with labels {} not selected by selector {}",
                    kt.getMetadata().getLabels(), selector);
                return false;
            }
            return true;
        }).collect(Collectors.partitioningBy(reconcilableTopic -> {
            boolean forDeletion = isForDeletion(reconcilableTopic.kt());
            if (forDeletion) {
                LOGGER.debugCr(reconcilableTopic.reconciliation(), "metadata.deletionTimestamp is set, so try onDelete()");
            }
            return forDeletion;
        }));

        var toBeDeleted = partitionedByDeletion.get(true);
        if (!toBeDeleted.isEmpty()) {
            deleteInternal(toBeDeleted, false);
        }

        // process remaining
        var remainingAfterDeletions = partitionedByDeletion.get(false);
        var timerSamples = remainingAfterDeletions.stream().collect(
            Collectors.toMap(identity(), rt -> startReconciliationTimer(metricsHolder)));
        var partitionedByManaged = remainingAfterDeletions.stream()
            .collect(Collectors.partitioningBy(reconcilableTopic -> TopicOperatorUtil.isManaged(reconcilableTopic.kt())));

        Results results = new Results();

        // process remaining unmanaged
        var unmanaged = partitionedByManaged.get(false);
        results.addAll(updatedUnmanagedTopics(unmanaged));

        // process remaining managed, skipping paused KTs
        var managed = partitionedByManaged.get(true);
        results.addAll(updateManagedTopics(reconcilableTopics, managed));

        updateStatuses(results);
        timerSamples.keySet().forEach(rt -> stopReconciliationTimer(metricsHolder, timerSamples.get(rt), config.namespace()));
        LOGGER.traceOp("Reconciled batch of {} KafkaTopics", results.size());
    }

    private Results updatedUnmanagedTopics(List<ReconcilableTopic> unmanagedTopics) {
        Results results = new Results();
        results.recordRightResults(addOrRemoveFinalizer(unmanagedTopics));
        metricsHolder.reconciliationsCounter(config.namespace()).increment(unmanagedTopics.size());
        return results;
    }

    private Results updateManagedTopics(List<ReconcilableTopic> batch, List<ReconcilableTopic> managedTopics) {
        var partitionedByPaused = validateManagedTopics(managedTopics).stream().filter(hasTopicSpec)
            .collect(Collectors.partitioningBy(reconcilableTopic -> isPaused(reconcilableTopic.kt())));

        Results results = new Results();

        List<ReconcilableTopic> paused = partitionedByPaused.get(true);
        results.recordRightResults(paused);

        List<ReconcilableTopic> mayNeedUpdate = partitionedByPaused.get(false);
        results.addAll(updateNonPausedTopics(batch, mayNeedUpdate));

        return results;
    }

    private Results updateNonPausedTopics(List<ReconcilableTopic> batch, List<ReconcilableTopic> topicsToUpdate) {
        metricsHolder.reconciliationsCounter(config.namespace()).increment(topicsToUpdate.size());
        var addedFinalizer = addOrRemoveFinalizer(topicsToUpdate);
        var currentStatesOrError = kafkaHandler.describeTopics(addedFinalizer);

        for (var reconcilableTopic : topicsToUpdate) {
            removeWarningConditions(reconcilableTopic, TopicOperatorException.Reason.INVALID_CONFIG, null);
            warnAboutThrottlingConfig(reconcilableTopic);
            warnAboutNonAlterableConfig(reconcilableTopic);
        }

        Results results = new Results();

        results.addAll(createMissingTopics(currentStatesOrError));

        var partitionedByHavingDifferentConfigs = partitionByHavingDifferentConfigs(currentStatesOrError.ok());

        // record topics which don't require configs changes (may be overwritten later)
        results.recordRightResults(partitionedByHavingDifferentConfigs.get(false).stream());
        // filter out topics whose configs can't be changed (e.g. throttling managed by CC, or ones prohibited by this operator's config)
        var actionableConfigChanges = retainActionableConfigChanges(partitionedByHavingDifferentConfigs.get(true));

        var partitionedByRequiredNewPartitions = partitionByRequiresNewPartitions(results, currentStatesOrError.ok());
        // add topics which don't require partitions changes to the results (may be overwritten later)
        results.recordRightResults(partitionedByRequiredNewPartitions.get(true).stream());
        var someCreatePartitions = partitionedByRequiredNewPartitions.get(false);

        // execute those updates
        var alterConfigsResults = kafkaHandler.alterConfigs(actionableConfigChanges);
        var createPartitionsResults = kafkaHandler.createPartitions(someCreatePartitions);
        var handleReplicasChangesResults = handleReplicasChanges(batch, currentStatesOrError.ok());

        results.recordResults(alterConfigsResults);
        results.recordResults(createPartitionsResults);
        results.recordResults(handleReplicasChangesResults);
        return results;
    }

    private Results createMissingTopics(PartitionedByError<ReconcilableTopic, TopicState> currentStatesOrError) {
        Results results = new Results();

        // partition the topics with errors by whether they're due to UnknownTopicOrPartitionException
        var partitionedByUnknownTopic = currentStatesOrError.errors().collect(Collectors.partitioningBy(pair -> {
            var ex = pair.getValue();
            return ex instanceof TopicOperatorException.KafkaError
                && ex.getCause() instanceof UnknownTopicOrPartitionException;
        }));

        // record the errors for those which are NOT due to UnknownTopicOrPartitionException
        results.recordLeftResults(partitionedByUnknownTopic.get(false).stream());

        // create topics in Kafka for those which ARE due to UnknownTopicOrPartitionException...
        var unknownTopics = partitionedByUnknownTopic.get(true).stream().map(Pair::getKey).toList();
        // ... and record those results.
        results.recordResults(kafkaHandler.createTopics(unknownTopics));
        return results;
    }

    private Map<Boolean, List<Pair<ReconcilableTopic, Collection<AlterConfigOp>>>> partitionByHavingDifferentConfigs(
        Stream<Pair<ReconcilableTopic, TopicState>> currentStates
    ) {
        // Determine config changes
        return currentStates.map(pair -> {
            var reconcilableTopic = pair.getKey();
            var currentState = pair.getValue();
            // determine config changes
            return new Pair<>(reconcilableTopic, buildAlterConfigOps(reconcilableTopic, currentState.configs()));
        }).collect(Collectors.partitioningBy(pair -> !pair.getValue().isEmpty()));
    }

    private Collection<AlterConfigOp> buildAlterConfigOps(ReconcilableTopic reconcilableTopic, Config configs) {
        Set<AlterConfigOp> alterConfigOps = new HashSet<>();
        if (hasConfig(reconcilableTopic.kt())) {
            for (var specConfigEntry : reconcilableTopic.kt().getSpec().getConfig().entrySet()) {
                String key = specConfigEntry.getKey();
                var specValueStr = configValueAsString(specConfigEntry.getValue());
                var kafkaConfigEntry = configs.get(key);
                if (kafkaConfigEntry == null
                    || !Objects.equals(specValueStr, kafkaConfigEntry.value())) {
                    alterConfigOps.add(new AlterConfigOp(
                        new ConfigEntry(key, specValueStr),
                        AlterConfigOp.OpType.SET));
                }
            }
        }
        HashSet<String> keysToRemove = configs.entries().stream()
            .filter(configEntry -> configEntry.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG)
            .map(ConfigEntry::name).collect(Collectors.toCollection(HashSet::new));
        if (hasConfig(reconcilableTopic.kt())) {
            keysToRemove.removeAll(reconcilableTopic.kt().getSpec().getConfig().keySet());
        }
        for (var key : keysToRemove) {
            alterConfigOps.add(new AlterConfigOp(
                new ConfigEntry(key, null),
                AlterConfigOp.OpType.DELETE));
        }

        return alterConfigOps;
    }

    private static String configValueAsString(Object value) {
        String valueStr;
        if (value instanceof String
            || value instanceof Boolean) {
            valueStr = value.toString();
        } else if (value instanceof Number) {
            valueStr = value.toString();
        } else if (value instanceof List) {
            valueStr = ((List<?>) value).stream()
                .map(BatchingTopicController::configValueAsString)
                .collect(Collectors.joining(","));
        } else {
            throw new RuntimeException("Cannot convert " + value);
        }
        return valueStr;
    }

    /**
     * Filters out config changes that should be ignored.
     *
     * @param alterConfigPairs Determined alter config operations.
     * @return Filtered alter config pairs.
     */
    private List<Pair<ReconcilableTopic, Collection<AlterConfigOp>>> retainActionableConfigChanges(
        List<Pair<ReconcilableTopic, Collection<AlterConfigOp>>> alterConfigPairs
    ) {
        var nonThrottlingConfigPairs = filterOutThrottlingConfig(alterConfigPairs);
        var alterableConfigPairs = filterOutNonAlterableConfig(nonThrottlingConfigPairs);

        return alterableConfigPairs.stream()
            .filter(pair -> !pair.getValue().isEmpty())
            .toList();
    }

    /**
     * <p>If Cruise Control integration is enabled, we ignore throttle configurations unless the user 
     * explicitly set them in {@code .spec.config}, but we always give a warning.</p>
     *
     * <p>This avoids the issue caused by the race condition between Cruise Control that dynamically sets 
     * them during throttled rebalances, and the operator that reverts them on periodic reconciliations.</p>
     *
     * @param alterConfigPairs Determined alter config operations.
     * @return Filtered alter config operations for this topic.
     */
    private List<Pair<ReconcilableTopic, Collection<AlterConfigOp>>> filterOutThrottlingConfig(List<Pair<ReconcilableTopic, Collection<AlterConfigOp>>> alterConfigPairs) {
        if (config.cruiseControlEnabled()) {
            return alterConfigPairs.stream()
                .map(pair -> {
                    var reconcilableTopic = pair.getKey();
                    Collection<AlterConfigOp> filteredOps = pair.getValue().stream()
                        .filter(op -> !(THROTTLING_CONFIG.contains(op.configEntry().name()) && !hasConfigProperty(reconcilableTopic.kt(), op.configEntry().name())))
                        .toList();
                    return new Pair<>(pair.getKey(), filteredOps);
                })
                .toList();
        } else {
            return alterConfigPairs;
        }
    }

    private void warnAboutThrottlingConfig(ReconcilableTopic reconcilableTopic) {
        if (config.cruiseControlEnabled()) {
            if (THROTTLING_CONFIG.stream().anyMatch(prop -> hasConfigProperty(reconcilableTopic.kt(), prop))) {
                THROTTLING_CONFIG.forEach(prop -> addWarningCondition(reconcilableTopic, TopicOperatorException.Reason.INVALID_CONFIG, format("Property %s may conflict with throttled rebalances", prop)));
            }
        }
    }

    /**
     * <p>The {@code STRIMZI_ALTERABLE_TOPIC_CONFIG} env var can be used to specify a comma separated list (allow list) 
     * of Kafka topic configurations that can be altered by users through {@code .spec.config}. Keep in mind that 
     * when changes are applied directly in Kafka, the operator will try to revert them with a warning.</p>
     *
     * <p>The default value is "ALL", which means no restrictions in changing {@code .spec.config}.
     * The opposite is "NONE", which can be set to explicitly disable any change.</p>
     *
     * <p>This is useful in standalone mode when you have a Kafka service that restricts 
     * alter operations to a subset of all the Kafka topic configurations.</p>
     *
     * @param alterConfigPairs All alter config operations.
     * @return Filtered alter config operations for this topic.
     */
    private List<Pair<ReconcilableTopic, Collection<AlterConfigOp>>> filterOutNonAlterableConfig(
        List<Pair<ReconcilableTopic, Collection<AlterConfigOp>>> alterConfigPairs
    ) {

        if (alterableConfigs == null) {
            return alterConfigPairs; // TODO this cannot be correct: returning the list for _all_ RT's if reconcilableTopic has null configs
        } else if (alterableConfigs.isEmpty()) {
            return List.of();
        } else {
            // check determined alter operations on update
            return alterConfigPairs.stream()
                .map(pair -> {
                    Collection<AlterConfigOp> filteredOps = pair.getValue().stream()
                        .filter(op -> {
                            String propName = op.configEntry().name();
                            return alterableConfigs.contains(propName) || THROTTLING_CONFIG.contains(propName);
                        })
                        .toList();
                    return new Pair<>(pair.getKey(), filteredOps);
                })
                .toList();
        }
    }

    private void warnAboutNonAlterableConfig(
        ReconcilableTopic reconcilableTopic
    ) {
        boolean hasNullConfigs = !hasConfig(reconcilableTopic.kt());
        if (alterableConfigs == null
            || hasNullConfigs) {
            return;
        }
        if (alterableConfigs.isEmpty()) {
            addWarningCondition(reconcilableTopic,
                TopicOperatorException.Reason.INVALID_CONFIG,
                "All properties are ignored according to alterable config");
        } else {
            reconcilableTopic.kt().getSpec().getConfig().keySet().stream()
                .filter(prop -> !alterableConfigs.contains(prop) && !THROTTLING_CONFIG.contains(prop))
                .sorted() // sorted to have a deterministic order for tests
                // check spec config on topic creation
                .forEach(prop -> addWarningCondition(reconcilableTopic, TopicOperatorException.Reason.INVALID_CONFIG,
                    format("Property %s is ignored according to alterable config", prop)));
        }
    }

    private Map<Boolean, List<Pair<ReconcilableTopic, NewPartitions>>> partitionByRequiresNewPartitions(
        Results results,
        Stream<Pair<ReconcilableTopic, TopicState>> currentStates
    ) {
        // Determine partition changes
        Stream<Pair<ReconcilableTopic, Either<TopicOperatorException, NewPartitions>>> partitionChanges = currentStates.map(pair -> {
            var reconcilableTopic = pair.getKey();
            var currentState = pair.getValue();
            // determine config changes
            return new Pair<>(reconcilableTopic, partitionChanges(reconcilableTopic.reconciliation(), reconcilableTopic.kt(), currentState.numPartitions()));
        });
        PartitionedByError<ReconcilableTopic, NewPartitions> newPartitionsOrError = partitionedByError(partitionChanges);
        results.recordLeftResults(newPartitionsOrError.errors());

        return newPartitionsOrError.ok().collect(
            Collectors.partitioningBy(pair -> pair.getValue() == null));
    }

    private static Either<TopicOperatorException, NewPartitions> partitionChanges(Reconciliation reconciliation, KafkaTopic kt, int currentNumPartitions) {
        int requested = kt.getSpec() == null || kt.getSpec().getPartitions() == null ? KafkaHandler.BROKER_DEFAULT : kt.getSpec().getPartitions();
        if (requested > currentNumPartitions) {
            LOGGER.debugCr(reconciliation, "Partition increase from {} to {}", currentNumPartitions, requested);
            return Either.ofRight(NewPartitions.increaseTo(requested));
        } else if (requested != KafkaHandler.BROKER_DEFAULT && requested < currentNumPartitions) {
            LOGGER.debugCr(reconciliation, "Partition decrease from {} to {}", currentNumPartitions, requested);
            return Either.ofLeft(new TopicOperatorException.NotSupported("Decreasing partitions not supported"));
        } else {
            LOGGER.debugCr(reconciliation, "No partition change");
            return Either.ofRight(null);
        }
    }

    private static boolean isForDeletion(KafkaTopic kt) {
        if (kt.getMetadata().getDeletionTimestamp() != null) {
            var deletionTimestamp = StatusUtils.isoUtcDatetime(kt.getMetadata().getDeletionTimestamp());
            var now = Instant.now();
            return !deletionTimestamp.isAfter(now);
        } else {
            return false;
        }
    }
    
    private List<ReconcilableTopic> addOrRemoveFinalizer(List<ReconcilableTopic> reconcilableTopics) {
        var collect = reconcilableTopics.stream()
            .map(reconcilableTopic -> new ReconcilableTopic(reconcilableTopic.reconciliation(),
                config.useFinalizer() 
                    ? kubernetesHandler.addFinalizer(reconcilableTopic) 
                    : kubernetesHandler.removeFinalizer(reconcilableTopic), reconcilableTopic.topicName()))
            .collect(Collectors.toList());
        LOGGER.traceOp("{} {} topics", config.useFinalizer() ? "Added finalizers to" : "Removed finalizers from", reconcilableTopics.size());
        return collect;
    }

    private Either<TopicOperatorException, Boolean> validate(ReconcilableTopic reconcilableTopic) {
        var doReconcile = Either.<TopicOperatorException, Boolean>ofRight(true);
        doReconcile = doReconcile.flatMapRight((Boolean x) -> x ? validateUnchangedTopicName(reconcilableTopic) : Either.ofRight(false));
        doReconcile = doReconcile.mapRight((Boolean x) -> x && rememberReconcilableTopic(reconcilableTopic));
        return doReconcile;
    }

    private boolean rememberReconcilableTopic(ReconcilableTopic reconcilableTopic) {
        var topicName = reconcilableTopic.topicName();
        var existing = topics.computeIfAbsent(topicName, k -> new ArrayList<>(1));
        KubeRef thisRef = new KubeRef(reconcilableTopic.kt());
        if (!existing.contains(thisRef)) {
            existing.add(thisRef);
        }
        return true;
    }

    private void forgetReconcilableTopic(ReconcilableTopic reconcilableTopic) {
        topics.compute(reconcilableTopic.topicName(), (k, v) -> {
            if (v != null) {
                v.remove(new KubeRef(reconcilableTopic.kt()));
                if (v.isEmpty()) {
                    return null;
                } else {
                    return v;
                }
            } else {
                return null;
            }
        });
    }

    /* test */ static boolean matchesSelector(Map<String, String> selector, Map<String, String> resourceLabels) {
        if (!selector.isEmpty()) {
            for (var selectorEntry : selector.entrySet()) {
                String resourceValue = resourceLabels.get(selectorEntry.getKey());
                if (resourceValue == null
                        || !resourceValue.equals(selectorEntry.getValue())) {
                    return false;
                }
            }
        }
        return resourceLabels.keySet().containsAll(selector.keySet());
    }

    private static Either<TopicOperatorException, Boolean> validateUnchangedTopicName(ReconcilableTopic reconcilableTopic) {
        if (reconcilableTopic.kt().getStatus() != null
                && reconcilableTopic.kt().getStatus().getTopicName() != null
                && !TopicOperatorUtil.topicName(reconcilableTopic.kt()).equals(reconcilableTopic.kt().getStatus().getTopicName())) {
            return Either.ofLeft(new TopicOperatorException.NotSupported("Changing spec.topicName is not supported"
            ));
        }
        return Either.ofRight(true);
    }
    
    private static boolean hasConfigProperty(KafkaTopic kt, String prop) {
        return hasConfig(kt) && kt.getSpec().getConfig().containsKey(prop);
    }

    private static boolean hasConditions(KafkaTopic kt) {
        return kt.getStatus() != null && kt.getStatus().getConditions() != null;
    }

    private static void addWarningCondition(ReconcilableTopic reconcilableTopic, TopicOperatorException.Reason reason, String message) {
        LOGGER.warnCr(reconcilableTopic.reconciliation(), message);
        List<Condition> conditions = hasConditions(reconcilableTopic.kt())
            ? reconcilableTopic.kt().getStatus().getConditions() : new ArrayList<>();
        if (conditions.stream().filter(c -> Objects.equals(c.getReason(), reason.value)
            && Objects.equals(c.getMessage(), message)).findFirst().isEmpty()) {
            conditions.add(new ConditionBuilder()
                .withReason(reason.value)
                .withMessage(message)
                .withStatus("True")
                .withType("Warning")
                .withLastTransitionTime(StatusUtils.iso8601Now())
                .build());
        }
        reconcilableTopic.kt().setStatus(
            new KafkaTopicStatusBuilder(reconcilableTopic.kt().getStatus())
                .withConditions(conditions)
                .build());
    }

    private static void removeWarningConditions(ReconcilableTopic reconcilableTopic, TopicOperatorException.Reason reason, String message) {
        List<Condition> conditions = hasConditions(reconcilableTopic.kt())
            ? reconcilableTopic.kt().getStatus().getConditions() : new ArrayList<>();
        if (message == null) {
            conditions.removeIf(c -> Objects.equals(c.getReason(), reason.value));
        } else {
            conditions.removeIf(c -> Objects.equals(c.getReason(), reason.value)
                && Objects.equals(c.getMessage(), message));
        }
        reconcilableTopic.kt().setStatus(
            new KafkaTopicStatusBuilder(reconcilableTopic.kt().getStatus())
                .withConditions(conditions)
                .build());
    }

    /**
     * Handles topic replicas changes.
     * 
     * <p>If Cruise Control integration is disabled, it returns an error for each change.</p>
     * 
     * <p>
     * If Cruise Control integration is enabled, it runs the following operations in order:
     * <ol>
     *     <li>Request new and pending changes</li>
     *     <li>Check the state of ongoing changes</li>
     *     <li>Complete pending but completed changes (*)</li>
     * </ol>
     * (*) A pending change needs to be completed by the reconciliation when it is unknown to Cruise Control
     * due to a restart, but the task actually completed successfully (i.e. the new replication factor
     * was applied), or when the user reverts an invalid replicas change to the previous value.
     * <p>
     *
     * @param reconcilableTopics Reconcilable topics from Kube.
     * @param currentStates Current topic state.
     * @return Reconcilable topics partitioned by error.
     */
    /* test */ PartitionedByError<ReconcilableTopic, Void> handleReplicasChanges(List<ReconcilableTopic> reconcilableTopics,
                                                                                 Stream<Pair<ReconcilableTopic, TopicState>> currentStates) {
        var differentRfResults = findDifferentRf(currentStates);
        Stream<Pair<ReconcilableTopic, Either<TopicOperatorException, Void>>> successStream;

        if (config.cruiseControlEnabled()) {
            var results = new HashSet<ReconcilableTopic>();
            var differentRfMap = differentRfResults.ok().map(Pair::getKey).toList()
                .stream().collect(Collectors.toMap(ReconcilableTopic::topicName, Function.identity()));

            var pending = topicsMatching(reconcilableTopics, this::isPendingReplicasChange);
            var brandNew = differentRfMap.values().stream()
                .filter(rt -> !isPendingReplicasChange(rt.kt()) && !isOngoingReplicasChange(rt.kt()))
                .toList();
            pending.addAll(brandNew);
            warnTooLargeMinIsr(pending);
            results.addAll(cruiseControlHandler.requestPendingChanges(pending));

            var ongoing = topicsMatching(reconcilableTopics, this::isOngoingReplicasChange);
            results.addAll(cruiseControlHandler.requestOngoingChanges(ongoing));

            var completed = pending.stream()
                .filter(rt -> !differentRfMap.containsKey(rt.topicName()) && !isFailedReplicasChange(rt.kt()))
                .collect(Collectors.toList());
            var reverted = pending.stream()
                .filter(rt -> !differentRfMap.containsKey(rt.topicName()) && isFailedReplicasChange(rt.kt()))
                .toList();
            completed.addAll(reverted);
            if (!completed.isEmpty()) {
                LOGGER.debugOp("Pending but completed replicas changes, Topics: {}", topicNames(completed));
            }
            completed.forEach(reconcilableTopic -> reconcilableTopic.kt().getStatus().setReplicasChange(null));
            results.addAll(completed);

            successStream = results.stream().map(reconcilableTopic -> new Pair<>(reconcilableTopic, Either.ofRight(null)));

        } else {
            successStream = differentRfResults.ok().map(pair -> {
                var reconcilableTopic = pair.getKey();
                var specReplicas = TopicOperatorUtil.replicas(reconcilableTopic.kt());
                var partitions = pair.getValue().partitionsWithDifferentRfThan(specReplicas);
                return new Pair<>(reconcilableTopic, Either.ofLeft(new TopicOperatorException.NotSupported(
                    "Replication factor change not supported, but required for partitions " + partitions)));
            });
        }

        Stream<Pair<ReconcilableTopic, Either<TopicOperatorException, Void>>> errorStream = differentRfResults.errors()
            .map(pair -> new Pair<>(pair.getKey(), Either.ofLeft(pair.getValue())));

        return TopicOperatorUtil.partitionedByError(Stream.concat(successStream, errorStream));
    }

    private PartitionedByError<ReconcilableTopic, TopicState> findDifferentRf(Stream<Pair<ReconcilableTopic, TopicState>> currentStates) {
        var apparentlyDifferentRf = currentStates.filter(pair -> {
            var reconcilableTopic = pair.getKey();
            var currentState = pair.getValue();
            return reconcilableTopic.kt().getSpec().getReplicas() != null
                && currentState.uniqueReplicationFactor() != reconcilableTopic.kt().getSpec().getReplicas();
        }).toList();

        return partitionedByError(kafkaHandler.filterByReassignmentTargetReplicas(apparentlyDifferentRf).stream());
    }

    /**
     * Cruise Control allows scale down of the replication factor under the min.insync.replicas value, which can cause 
     * disruption to producers with acks=all. When this happens, the Topic Operator won't block the operation, but will 
     * just log a warning, because the KafkaRoller ignores topics with RF < minISR, and they don't even show up as under 
     * replicated in Kafka metrics.
     *
     * @param reconcilableTopics Reconcilable topic.
     */
    private void warnTooLargeMinIsr(List<ReconcilableTopic> reconcilableTopics) {
        if (config.skipClusterConfigReview()) {
            // This method is for internal configurations. So skipping.
            return;
        }

        var clusterMinIsr = kafkaHandler.clusterConfig(KafkaHandler.MIN_INSYNC_REPLICAS);
        for (ReconcilableTopic reconcilableTopic : reconcilableTopics) {
            var topicConfig = reconcilableTopic.kt().getSpec().getConfig();
            if (topicConfig != null) {
                Integer topicMinIsr = (Integer) topicConfig.get(KafkaHandler.MIN_INSYNC_REPLICAS);
                int minIsr = topicMinIsr != null ? topicMinIsr : clusterMinIsr.map(Integer::parseInt).orElse(1);
                int targetRf = reconcilableTopic.kt().getSpec().getReplicas();
                if (targetRf < minIsr) {
                    LOGGER.warnCr(reconcilableTopic.reconciliation(),
                        "The target replication factor ({}) is below the configured {} ({})", targetRf, KafkaHandler.MIN_INSYNC_REPLICAS, minIsr);
                }
            }
        }
    }

    private List<ReconcilableTopic> topicsMatching(List<ReconcilableTopic> reconcilableTopics, Predicate<KafkaTopic> status) {
        return reconcilableTopics.stream().filter(rt -> status.test(rt.kt())).collect(Collectors.toList());
    }

    private boolean isPendingReplicasChange(KafkaTopic kafkaTopic) {
        return TopicOperatorUtil.hasReplicasChange(kafkaTopic.getStatus())
            && kafkaTopic.getStatus().getReplicasChange().getState() == PENDING
            && kafkaTopic.getStatus().getReplicasChange().getSessionId() == null;
    }

    private boolean isOngoingReplicasChange(KafkaTopic kafkaTopic) {
        return TopicOperatorUtil.hasReplicasChange(kafkaTopic.getStatus())
            && kafkaTopic.getStatus().getReplicasChange().getState() == ONGOING
            && kafkaTopic.getStatus().getReplicasChange().getSessionId() != null;
    }

    private boolean isFailedReplicasChange(KafkaTopic kafkaTopic) {
        return TopicOperatorUtil.hasReplicasChange(kafkaTopic.getStatus())
            && kafkaTopic.getStatus().getReplicasChange().getState() == PENDING
            && kafkaTopic.getStatus().getReplicasChange().getMessage() != null;
    }

    private final Predicate<ReconcilableTopic> hasTopicSpec = reconcilableTopic -> {
        var hasSpec = reconcilableTopic.kt().getSpec() != null;
        if (!hasSpec) {
            LOGGER.warnCr(reconcilableTopic.reconciliation(), "Topic has no spec.");
        }
        return hasSpec;
    };

    private List<ReconcilableTopic> validateManagedTopics(List<ReconcilableTopic> managed) {
        return managed.stream().filter(reconcilableTopic -> {
            var e = validate(reconcilableTopic);
            if (e.isRightEqual(false)) {
                // Do nothing
                return false;
            } else if (e.isRightEqual(true)) {
                return true;
            } else {
                updateStatusForException(reconcilableTopic, e.left());
                return false;
            }
        }).filter(reconcilableTopic -> {
            var e = validateSingleManagingResource(reconcilableTopic);
            if (e.isRightEqual(false)) {
                // Do nothing
                return false;
            } else if (e.isRightEqual(true)) {
                return true;
            } else {
                updateStatusForException(reconcilableTopic, e.left());
                return false;
            }
        }).toList();
    }

    private Either<TopicOperatorException, Boolean> validateSingleManagingResource(ReconcilableTopic reconcilableTopic) {
        var topicName = reconcilableTopic.topicName();
        var existing = topics.get(topicName);
        var thisRef = new KubeRef(reconcilableTopic.kt());
        if (existing.size() != 1) {
            var byCreationTime = existing.stream().sorted(Comparator.comparing(KubeRef::creationTime)).toList();

            var oldest = byCreationTime.get(0);
            var nextOldest = byCreationTime.size() >= 2 ? byCreationTime.get(1) : null;
            TopicOperatorException e = new TopicOperatorException.ResourceConflict("Managed by " + oldest);
            if (nextOldest == null) {
                // This is only resource for that topic => it is the unique oldest
                return Either.ofRight(true);
            } else if (thisRef.equals(oldest) && nextOldest.creationTime() != oldest.creationTime()) {
                // This resource is the unique oldest, so it's OK.
                // The others will eventually get reconciled and put into ResourceConflict
                return Either.ofRight(true);
            } else if (thisRef.equals(oldest)
                && reconcilableTopic.kt().getStatus() != null
                && reconcilableTopic.kt().getStatus().getConditions() != null
                && reconcilableTopic.kt().getStatus().getConditions().stream().anyMatch(c -> "Ready".equals(c.getType()) && "True".equals(c.getStatus()))) {
                return Either.ofRight(true);
            } else {
                // Return an error putting this resource into ResourceConflict
                return Either.ofLeft(e);
            }

        }
        return Either.ofRight(true);
    }

    private void updateStatuses(Results results) {
        // update statues with the overall results
        results.forEachRightResult((reconcilableTopic, ignored) ->
            updateStatusForSuccess(reconcilableTopic)
        );
        results.forEachLeftResult(this::updateStatusForException);
        LOGGER.traceOp("Updated status of {} KafkaTopics", results.size());
    }

    private void updateStatusForSuccess(ReconcilableTopic reconcilableTopic) {
        List<Condition> conditions = hasConditions(reconcilableTopic.kt())
            ? reconcilableTopic.kt().getStatus().getConditions() : new ArrayList<>();
        // ready/paused condition is always first
        conditions.removeIf(c -> Set.of("ReconciliationPaused", "Ready").contains(c.getType()));
        conditions.add(0, new ConditionBuilder()
            .withType(isPaused(reconcilableTopic.kt()) ? "ReconciliationPaused" : "Ready")
            .withStatus("True")
            .withLastTransitionTime(StatusUtils.iso8601Now())
            .build());
        reconcilableTopic.kt().setStatus(
            new KafkaTopicStatusBuilder(reconcilableTopic.kt().getStatus())
                .withConditions(conditions)
                .build());
        kubernetesHandler.updateStatus(reconcilableTopic);
        metricsHolder.successfulReconciliationsCounter(config.namespace()).increment();
    }

    private void updateStatusForException(ReconcilableTopic reconcilableTopic, Exception e) {
        String reason;
        if (e instanceof TopicOperatorException) {
            LOGGER.warnCr(reconcilableTopic.reconciliation(), "Reconciliation failed: {}", e.getMessage());
            reason = ((TopicOperatorException) e).reason();
        } else {
            LOGGER.errorCr(reconcilableTopic.reconciliation(), "Reconciliation failed with unexpected exception", e);
            reason = e.getClass().getSimpleName();
        }
        reconcilableTopic.kt().setStatus(
            new KafkaTopicStatusBuilder(reconcilableTopic.kt().getStatus())
                .withConditions(List.of(new ConditionBuilder()
                    .withType("Ready")
                    .withStatus("False")
                    .withReason(reason)
                    .withMessage(e.getMessage())
                    .withLastTransitionTime(StatusUtils.iso8601Now())
                    .build()))
                .build());
        kubernetesHandler.updateStatus(reconcilableTopic);
        metricsHolder.failedReconciliationsCounter(config.namespace()).increment();
    }
}
