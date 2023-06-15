/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

/**
 * Record used to keep reference to a Kafka node through its pod name and node ID.
 *
 * @param podName       Name of the pod which represents this node
 * @param nodeId        ID of the Kafka node
 * @param poolName      Name of the pool this node belongs to
 * @param controller    Identifies if this node is a KRaft controller or not
 * @param broker        Identifies if this node is a broker or not
 */
public record NodeRef(String podName, int nodeId, String poolName, boolean controller, boolean broker) {
    @Override
    public String toString() {
        return podName + "/" + nodeId;
    }
}
