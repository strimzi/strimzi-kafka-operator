/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.model;

import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.topic.ReplicasChangeStatus;
import org.apache.kafka.clients.admin.AlterConfigOp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

/**
 * This class is used to accumulate reconciliation results and associated state.
 * It allows methods to return intermediate results, rather than mutating collections passed to them as arguments.
 * Results can be merged with other results coming from a different method.
 */
public class Results {
    private final Map<ReconcilableTopic, Either<TopicOperatorException, Object>> results = new HashMap<>();
    private final Map<ReconcilableTopic, Collection<Condition>> conditions = new HashMap<>();
    private final Map<ReconcilableTopic, ReplicasChangeStatus> replicasChanges = new HashMap<>();
    private final List<Pair<ReconcilableTopic, Collection<AlterConfigOp>>> configChanges = new ArrayList<>();
    
    /**
     * Adds given results, except for topics which already have an error recorded.
     * 
     * @param partitionedByError Results partitioned by error.
     */
    public void addResults(PartitionedByError<ReconcilableTopic, ?> partitionedByError) {
        addRightResults(partitionedByError.ok());
        addLeftResults(partitionedByError.errors());
    }

    /**
     * Adds successful results from a stream.
     *
     * @param ok Success stream.
     */
    public void addRightResults(Stream<? extends Pair<ReconcilableTopic, ?>> ok) {
        ok.forEach(pair -> addResult(pair.getKey(), Either.ofRight(null)));
    }

    /**
     * Adds successful results from a collection.
     *
     * @param ok Success stream.
     */
    public void addRightResults(Collection<ReconcilableTopic> ok) {
        ok.forEach(rt -> addResult(rt, Either.ofRight(null)));
    }

    /**
     * Adds error results from a stream.
     *
     * @param errors Error stream.
     */
    public void addLeftResults(Stream<Pair<ReconcilableTopic, TopicOperatorException>> errors) {
        errors.forEach(pair -> addResult(pair.getKey(), Either.ofLeft(pair.getValue())));
    }

    private void addResult(ReconcilableTopic key, Either<TopicOperatorException, Object> result) {
        results.compute(key, (k, v) -> {
            if (v == null) {
                // use given result if there is no existing result
                return result;
            } else if (v.isRight()) {
                // if the existing result was success use the given result (errors beat successes)
                return result;
            } else {
                // otherwise the existing result must be an error, 
                // the given result might also be an error, but "first error wins"
                return v;
            }
        });
    }

    /**
     * Gets the number of reconciliations.
     *
     * @return Number of reconciliations.
     */
    public int size() {
        return results.size();
    }

    /**
     * Performs an action for each successful result.
     *
     * @param action Success action.
     */
    public void forEachRightResult(BiConsumer<ReconcilableTopic, Object> action) {
        results.forEach((reconcilableTopic, either) -> {
            if (either.isRight()) {
                action.accept(reconcilableTopic, either.right());
            }
        });
    }

    /**
     * Performs an action for each error result.
     *
     * @param action Error action.
     */
    public void forEachLeftResult(BiConsumer<ReconcilableTopic, TopicOperatorException> action) {
        results.forEach((reconcilableTopic, either) -> {
            if (!either.isRight()) {
                action.accept(reconcilableTopic, either.left());
            }
        });
    }

    /**
     * Merge two results into a single instance.
     * 
     * @param other Results.
     */
    public void merge(Results other) {
        for (var entry : other.results.entrySet()) {
            addResult(entry.getKey(), entry.getValue());
        }
        addConditions(other.getConditions());
        if (other.getReplicasChanges() != null) {
            addRightResults(other.getReplicasChanges().keySet());
            addReplicasChanges(other.getReplicasChanges());
        }
        if (other.getConfigChanges() != null) {
            this.replaceConfigChanges(other.getConfigChanges());
        }
    }

    private Map<ReconcilableTopic, Collection<Condition>> getConditions() {
        return this.conditions;
    }

    /**
     * Gets the status conditions for a reconcilable topic.
     *
     * @param reconcilableTopic Reconcilable topic.
     * @return Status conditions for this reconcilable topic.
     */
    public Collection<Condition> getConditions(ReconcilableTopic reconcilableTopic) {
        return this.conditions.getOrDefault(reconcilableTopic, List.of());
    }
    
    private void addConditions(Map<ReconcilableTopic, Collection<Condition>> conditions) {
        conditions.forEach(this::addConditions);
    }

    /**
     * Adds conditions for a reconcilable topic.
     *
     * @param reconcilableTopic Reconcilable topic.
     * @param conditions Conditions.
     */
    public void addConditions(ReconcilableTopic reconcilableTopic, Collection<Condition> conditions) {
        this.conditions.computeIfAbsent(reconcilableTopic, k -> new ArrayList<>()).addAll(conditions);
    }
    
    /**
     * Adds a single condition for a reconcilable topic.
     *
     * @param reconcilableTopic Reconcilable topic.
     * @param condition Condition.
     */
    public void addCondition(ReconcilableTopic reconcilableTopic, Condition condition) {
        this.conditions.computeIfAbsent(reconcilableTopic, k -> new ArrayList<>()).add(condition);
    }

    /**
     * Gets all replicas change statuses.
     *
     * @return Replicas change statuses.
     */
    public Map<ReconcilableTopic, ReplicasChangeStatus> getReplicasChanges() {
        return replicasChanges;
    }

    /**
     * Gets the replicas change status for a reconcilable topic.
     *
     * @param reconcilableTopic Reconcilable topic.
     * @return Replicas change status.
     */
    public ReplicasChangeStatus getReplicasChange(ReconcilableTopic reconcilableTopic) {
        return replicasChanges.get(reconcilableTopic);
    }
    
    /**
     * Adds multiple replicas change statuses.
     *
     * @param replicasChangeStatus Replicas change status.
     */
    public void addReplicasChanges(Map<ReconcilableTopic, ReplicasChangeStatus> replicasChangeStatus) {
        replicasChangeStatus.forEach(this::addReplicasChange);
    }
    
    /**
     * Adds a replicas change status for a reconcilable topic.
     *
     * @param reconcilableTopic Reconcilable topic.
     * @param replicasChangeStatus Replicas change status.
     */
    public void addReplicasChange(ReconcilableTopic reconcilableTopic, ReplicasChangeStatus replicasChangeStatus) {
        this.replicasChanges.put(reconcilableTopic, replicasChangeStatus);
    }

    /**
     * Gets all alter config operations.
     *
     * @return All alter config ops.
     */
    public List<Pair<ReconcilableTopic, Collection<AlterConfigOp>>> getConfigChanges() {
        return this.configChanges;
    }

    /**
     * Replaces all configuration changes.
     *
     * @param configChanges Alter config ops.
     */
    public void replaceConfigChanges(List<Pair<ReconcilableTopic, Collection<AlterConfigOp>>> configChanges) {
        this.configChanges.clear();
        this.configChanges.addAll(configChanges);
    }
}
