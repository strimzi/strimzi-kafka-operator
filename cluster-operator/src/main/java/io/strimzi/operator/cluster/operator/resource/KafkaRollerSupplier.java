/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import java.util.List;
import java.util.function.Function;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.common.Reconciliation;

public interface KafkaRollerSupplier {
    KafkaRoller kafkaRoller(Reconciliation reconciliation, int replicas,
                            Secret clusterCaCertSecret,
                            Secret coKeySecret,
                            KafkaCluster kafkaCluster,
                            String kafkaLogging,
                            Function<Pod, List<String>> rollPodAndLogReason);
}

