/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.metrics;

/**
 * Metric key for metrics specified by kind, namespace and resource name.
 */
public class ResourceMetricKey extends MetricKey {
    private final String name;

    /**
     * Constructor
     *
     * @param kind      Kind of the resource
     * @param namespace Namespace of the resource
     * @param name      Name of the resource
     */
    public ResourceMetricKey(String kind, String namespace, String name) {
        super(kind, namespace);

        this.name = name;
    }

    @Override
    public String getKey() {
        return String.format("%s/%s/%s", kind, namespace, name);
    }

    /**
     * Returns the name of the resource.
     *
     * @return Name of the resource
     */
    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object obj) {
        // The key includes the resource name, so the comparison based on the key from the parent class is sufficient
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public String toString() {
        return "ResourceMetricKey{" +
                "kind='" + kind + '\'' +
                ", namespace='" + namespace + '\'' +
                ", name='" + name + '\'' +
                "}";
    }
}
