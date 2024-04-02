/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.metrics;

/**
 * Metric key for metrics specified by kind, namespace and cluster name.
 */
public class CertificateMetricKey extends MetricKey {
    /**
     * The resource type the CA belongs to
     */
    public enum Type {
        /**
         * Client CA
         */
        CLIENT_CA("client-ca"),
        /**
         * Cluster CA
         */
        CLUSTER_CA("cluster-ca");

        /**
         * Display name
         */
        private final String displayName;

        Type(String displayName) {
            this.displayName = displayName;
        }

        /**
         * Get the display name of the type
         *
         * @return Display name
         */
        public String getDisplayName() {
            return displayName;
        }
    }

    private final String clusterName;
    private final Type type;

    /**
     * Constructor
     *
     * @param kind          Kind of the resource
     * @param namespace     Namespace of the resource
     * @param clusterName   Name of the cluster
     * @param type          Type of the CA
     */
    public CertificateMetricKey(String kind, String namespace, String clusterName, Type type) {
        super(kind, namespace);

        this.clusterName = clusterName;
        this.type = type;
    }

    @Override
    public String getKey() {
        return String.format("%s/%s/%s/%s", kind, namespace, clusterName, getCaType());
    }

    /**
     * Get the cluster name
     *
     * @return  Cluster name
     */
    public String getClusterName() {
        return clusterName;
    }

    /**
     * Get the CA type
     *
     * @return  CA type
     */
    public String getCaType() {
        return type.getDisplayName();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CertificateMetricKey other) {
            return super.equals(obj) && this.clusterName.equals(other.clusterName) && this.type.equals(other.type);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return super.hashCode() + this.clusterName.hashCode() + this.type.hashCode();
    }

    @Override
    public String toString() {
        return "CaCertificateMetricKey(" +
                "kind=" + kind +
                ", namespace=" + namespace +
                ", clusterName=" + clusterName +
                ", type=" + type +
                ')';
    }
}
