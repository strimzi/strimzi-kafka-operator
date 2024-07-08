/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.model;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

/**
 * Reconciliation results.
 */
public class Results {
    private final Map<ReconcilableTopic, Either<TopicOperatorException, Object>> results = new HashMap<>();

    /**
     * @param partitionedByError Results to record.
     */
    public void recordResults(PartitionedByError<ReconcilableTopic, ? extends Object> partitionedByError) {
        recordRightResults(partitionedByError.ok());
        recordLeftResults(partitionedByError.errors());
    }

    /**
     * @param ok Success results to record.
     */
    public void recordRightResults(Stream<? extends Pair<ReconcilableTopic, ? extends Object>> ok) {
        ok.forEach(pair -> recordResult(pair.getKey(), Either.ofRight(null)));
    }

    /**
     * @param ok Success result to record.
     */
    public void recordRightResults(Collection<ReconcilableTopic> ok) {
        ok.forEach(rt -> recordResult(rt, Either.ofRight(null)));
    }

    /**
     * @param errors Error result to record.
     */
    public void recordLeftResults(Stream<Pair<ReconcilableTopic, TopicOperatorException>> errors) {
        errors.forEach(pair -> recordResult(pair.getKey(), Either.ofLeft(pair.getValue())));
    }
    
    private void recordResult(ReconcilableTopic key,
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
     * @return Results size.
     */
    public int size() {
        return results.size();
    }

    /**
     * @param action Action to execute for each success result.
     */
    public void forEachRightResult(BiConsumer<ReconcilableTopic, Object> action) {
        results.forEach((reconcilableTopic, either) -> {
            if (either.isRight()) {
                action.accept(reconcilableTopic, either.right());
            }
        });
    }

    /**
     * @param action Action to execute for each failure result.
     */
    public void forEachLeftResult(BiConsumer<ReconcilableTopic, TopicOperatorException> action) {
        results.forEach((reconcilableTopic, either) -> {
            if (!either.isRight()) {
                action.accept(reconcilableTopic, either.left());
            }
        });
    }

    /**
     * @param results Results to add.
     */
    public void addAll(Results results) {
        for (var entry : results.results.entrySet()) {
            this.recordResult(entry.getKey(), entry.getValue());
        }
    }
}
