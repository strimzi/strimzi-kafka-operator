/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

/**
 * Represents the configuration details of a remote Kubernetes cluster
 * required by the Cluster Operator to manage Kafka nodes across clusters.
 *
 * @param clusterId   The ID of the remote Kubernetes cluster.
 * @param apiUrl      The URL of the remote Kubernetes API server.
 * @param secretName  The name of the Kubernetes Secret used for authentication.
 */
public record ClusterInfo(String clusterId, String apiUrl, String secretName) {
}
