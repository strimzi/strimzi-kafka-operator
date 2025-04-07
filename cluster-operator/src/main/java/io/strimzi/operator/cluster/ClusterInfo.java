/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

/**
 * Represents the configuration details of a remote Kubernetes cluster
 * required by the Cluster Operator to manage Kafka nodes across clusters.
 *
 * Each remote cluster is identified by a unique cluster ID, and is associated
 * with a Kubernetes API server URL and the name of a Kubernetes Secret
 * containing authentication credentials.
 */
public class ClusterInfo {
    private String clusterId;
    private String apiUrl;
    private String secretName;

    /**
     * Constructs an empty ClusterInfo instance.
     * Required for creating instances using default constructor (e.g. during parsing).
     */
    public ClusterInfo() {
    }

    /**
     * Constructs a ClusterInfo instance with the given cluster ID, API URL, and Secret name.
     *
     * @param clusterId  The ID of the remote Kubernetes cluster.
     * @param apiUrl     The URL of the remote Kubernetes API server.
     * @param secretName The name of the Kubernetes Secret used for authentication.
     */
    public ClusterInfo(String clusterId, String apiUrl, String secretName) {
        this.clusterId = clusterId;
        this.apiUrl = apiUrl;
        this.secretName = secretName;
    }

    /**
     * Gets the cluster ID.
     *
     * @return The cluster ID.
     */
    public String getClusterId() {
        return clusterId;
    }

    /**
     * Sets the cluster ID.
     *
     * @param clusterId The cluster ID.
     */
    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    /**
     * Gets the URL of the remote Kubernetes API server.
     *
     * @return The Kubernetes cluster API server URL.
     */
    public String getApiUrl() {
        return apiUrl;
    }

    /**
     * Sets the URL of the remote Kubernetes API server.
     *
     * @param apiUrl The Kubernetes cluster API server URL.
     */
    public void setApiUrl(String apiUrl) {
        this.apiUrl = apiUrl;
    }

    /**
     * Gets the name of the Kubernetes Secret used for authentication.
     *
     * @return The Secret name.
     */
    public String getSecretName() {
        return secretName;
    }

    /**
     * Sets the name of the Kubernetes Secret used for authentication.
     *
     * @param secretName The Secret name.
     */
    public void setSecretName(String secretName) {
        this.secretName = secretName;
    }
}
