/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.metrics;

/**
 * Abstract class for metric keys used for caching.
 */
public class MetricKey {

    protected final String kind;
    protected final String namespace;

    /**
     * Constructor
     *
     * @param kind      Kind of the resource
     * @param namespace Namespace of the resource
     */
    public MetricKey(String kind, String namespace) {
        this.kind = kind;
        this.namespace = namespace;
    }

    /**
     * Returns the key of the metric.
     *
     * @return  Key of the metric
     */
    public String getKey() {
        return String.format("%s/%s", kind, namespace);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof MetricKey metricKey) {
            return getKey().equals(metricKey.getKey());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return getKey().hashCode();
    }

    @Override
    public String toString() {
        return "MetricKey{" +
                "kind='" + kind + '\'' +
                ", namespace='" + namespace + '\'' +
                "}";
    }

    /**
     * Returns the kind of the resource.
     *
     * @return Kind of the resource
     */
    public String getKind() {
        return kind;
    }

    /**
     * Returns the namespace of the resource.
     *
     * @return Namespace of the resource
     */
    public String getNamespace() {
        return namespace;
    }
}
