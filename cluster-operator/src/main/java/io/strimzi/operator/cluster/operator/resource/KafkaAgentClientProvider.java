/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.operator.common.Reconciliation;

/**
 * Helper interface to pass different KafkaAgentClient implementations
 */
public interface KafkaAgentClientProvider {

    /**
     * Creates an instance of KafkaAgentClient
     *
     * @param reconciliation    Reconciliation information
     * @param clusterCaCertSecret   Secret with the Cluster CA public key
     * @param coKeySecret   Secret with the Cluster CA private key
     *
     * @return  KafkaAgentClient instance
     */
    KafkaAgentClient createKafkaAgentClient(Reconciliation reconciliation, Secret clusterCaCertSecret, Secret coKeySecret);
}
