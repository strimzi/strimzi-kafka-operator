/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.operator.common.Reconciliation;

/**
 * Class to provide the real KafkaAgentClient which connects to actual Kafka Agent
 */
public class DefaultKafkaAgentClientProvider implements KafkaAgentClientProvider {

    @Override
    public KafkaAgentClient createKafkaAgentClient(Reconciliation reconciliation, Secret clusterCaCertSecret, Secret coKeySecret) {
        return new KafkaAgentClient(reconciliation, reconciliation.name(), reconciliation.namespace(), clusterCaCertSecret, coKeySecret);
    }
}
