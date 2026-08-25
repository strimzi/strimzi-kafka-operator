/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.metrics;

/**
 * Metric key for anomaly detection metrics, keyed by kind, namespace, and fixability.
 */
public class AnomalyMetricKey extends MetricKey {

    private final String fixability;

    /**
     * Constructor
     *
     * @param kind          Kind of the resource
     * @param namespace     Namespace of the resource
     * @param fixability    Fixability classification of the anomaly
     */
    public AnomalyMetricKey(String kind, String namespace, String fixability) {
        super(kind, namespace);
        this.fixability = fixability;
    }

    @Override
    public String getKey() {
        return String.format("%s/%s/%s", kind, namespace, fixability);
    }

    /**
     * Get the fixability classification
     *
     * @return  Fixability classification
     */
    public String getFixability() {
        return fixability;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AnomalyMetricKey other) {
            return super.equals(obj) && this.fixability.equals(other.fixability);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return super.hashCode() + this.fixability.hashCode();
    }

    @Override
    public String toString() {
        return "AnomalyMetricKey(" +
                "kind=" + kind +
                ", namespace=" + namespace +
                ", fixability=" + fixability +
                ')';
    }
}
