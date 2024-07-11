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
 * Reconciliation results.
 */
public class Results {
    // results from internal operations
    private final Map<ReconcilableTopic, Either<TopicOperatorException, Object>> results = new HashMap<>();
    
    // pending status updates
    private final Map<ReconcilableTopic, Collection<Condition>> conditions = new HashMap<>();
    private final Map<ReconcilableTopic, ReplicasChangeStatus> replicasChanges = new HashMap<>();

    // pending alter config changes
    private List<Pair<ReconcilableTopic, Collection<AlterConfigOp>>> configChanges = new ArrayList<>();
    
    /**
     * @param partitionedByError Results partitioned by error.
     */
    public void setResults(PartitionedByError<ReconcilableTopic, ? extends Object> partitionedByError) {
        setRightResults(partitionedByError.ok());
        setLeftResults(partitionedByError.errors());
    }

    /**
     * @param ok Success results.
     */
    public void setRightResults(Stream<? extends Pair<ReconcilableTopic, ? extends Object>> ok) {
        ok.forEach(pair -> setResult(pair.getKey(), Either.ofRight(null)));
    }

    /**
     * @param ok Success results.
     */
    public void setRightResults(Collection<ReconcilableTopic> ok) {
        ok.forEach(rt -> setResult(rt, Either.ofRight(null)));
    }

    /**
     * @param errors Error results.
     */
    public void setLeftResults(Stream<Pair<ReconcilableTopic, TopicOperatorException>> errors) {
        errors.forEach(pair -> setResult(pair.getKey(), Either.ofLeft(pair.getValue())));
    }

    private void setResult(ReconcilableTopic key,
                           Either<TopicOperatorException, Object> result) {
        results.compute(key, (k, v) -> {
            if (v == null) { // use given result if there is no existing result
                return result;
            } else if (v.isRight()) { // if the existing result was success use the given result (errors beat successes)
                return result;
            } else { // otherwise the existing result must be an error, the given result might also be an error, but "first error wins"
                return v;
            }
        });
    }

    /**
     * @return Number of results.
     */
    public int size() {
        return results.size();
    }

    /**
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
     * @param results Results.
     */
    public void addAll(Results results) {
        for (var entry : results.results.entrySet()) {
            this.setResult(entry.getKey(), entry.getValue());
        }
        this.setConditions(results.getConditions());
        if (results.getReplicasChanges() != null) {
            this.setRightResults(results.getReplicasChanges().keySet());
            this.setReplicasChanges(results.getReplicasChanges());
        }
        if (results.getConfigChanges() != null) {
            this.setConfigChanges(results.getConfigChanges());
        }
    }

    /**
     * @return All status conditions.
     */
    public Map<ReconcilableTopic, Collection<Condition>> getConditions() {
        return this.conditions;
    }

    /**
     * @param reconcilableTopic Reconcilable topic.
     * @return Status conditions for this reconcilable topic.
     */
    public Collection<Condition> getConditions(ReconcilableTopic reconcilableTopic) {
        return this.conditions.getOrDefault(reconcilableTopic, List.of());
    }

    /**
     * @param conditions Status conditions.
     */
    public void setConditions(Map<ReconcilableTopic, Collection<Condition>> conditions) {
        conditions.forEach((reconcilableTopic, tc) -> setConditions(reconcilableTopic, tc));
    }

    /**
     * @param reconcilableTopic Reconcilable topic.
     * @param conditions Conditions.
     */
    public void setConditions(ReconcilableTopic reconcilableTopic, Collection<Condition> conditions) {
        this.conditions.computeIfAbsent(reconcilableTopic, k -> new ArrayList<>()).addAll(conditions);
    }
    
    /**
     * @param reconcilableTopic Reconcilable topic.
     * @param condition Condition.
     */
    public void setCondition(ReconcilableTopic reconcilableTopic, Condition condition) {
        this.conditions.computeIfAbsent(reconcilableTopic, k -> new ArrayList<>()).add(condition);
    }

    /**
     * @return All replicas change statuses.
     */
    public Map<ReconcilableTopic, ReplicasChangeStatus> getReplicasChanges() {
        return replicasChanges;
    }
    
    /**
     * @param reconcilableTopic Reconcilable topic.
     * @return Replicas change status.
     */
    public ReplicasChangeStatus getReplicasChange(ReconcilableTopic reconcilableTopic) {
        return replicasChanges.get(reconcilableTopic);
    }
    
    /**
     * @param replicasChangeStatus Replicas change status.
     */
    public void setReplicasChanges(Map<ReconcilableTopic, ReplicasChangeStatus> replicasChangeStatus) {
        replicasChangeStatus.forEach((reconcilableTopic, status) -> setReplicasChange(reconcilableTopic, status));
    }
    
    /**
     * @param reconcilableTopic Reconcilable topic.
     * @param replicasChangeStatus Replicas change status.
     */
    public void setReplicasChange(ReconcilableTopic reconcilableTopic, ReplicasChangeStatus replicasChangeStatus) {
        this.replicasChanges.put(reconcilableTopic, replicasChangeStatus);
    }

    /**
     * @param configChanges Alter config ops.
     */
    public void setConfigChanges(List<Pair<ReconcilableTopic, Collection<AlterConfigOp>>> configChanges) {
        this.configChanges = configChanges;
    }

    /**
     * @return All alter config ops.
     */
    public List<Pair<ReconcilableTopic, Collection<AlterConfigOp>>> getConfigChanges() {
        return this.configChanges;
    }
}
