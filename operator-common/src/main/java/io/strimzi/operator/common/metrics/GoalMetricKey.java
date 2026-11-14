/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.metrics;

/**
 * Metric key for per-goal auto-rebalance triggered metrics, keyed by kind, namespace, and goal name.
 */
public class GoalMetricKey extends MetricKey {

    private final String goal;

    /**
     * Constructor
     *
     * @param kind       Kind of the resource
     * @param namespace  Namespace of the resource
     * @param goal       Name of the violated goal that triggered the rebalance
     */
    public GoalMetricKey(String kind, String namespace, String goal) {
        super(kind, namespace);
        this.goal = goal;
    }

    @Override
    public String getKey() {
        return String.format("%s/%s/%s", kind, namespace, goal);
    }

    /**
     * Get the goal name
     *
     * @return  Goal name
     */
    public String getGoal() {
        return goal;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof GoalMetricKey other) {
            return super.equals(obj) && this.goal.equals(other.goal);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return super.hashCode() + this.goal.hashCode();
    }

    @Override
    public String toString() {
        return "GoalMetricKey(" +
                "kind=" + kind +
                ", namespace=" + namespace +
                ", goal=" + goal +
                ')';
    }
}
