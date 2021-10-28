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
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.vertx.core.Vertx;

public class DefaultKafkaRollerSupplier implements KafkaRollerSupplier {

    private final Vertx vertx;
    private final PodOperator podOps;
    private final AdminClientProvider adminClientProvider;
    private final long operationTimeoutMs;

    public DefaultKafkaRollerSupplier(Vertx vertx, PodOperator podOps, AdminClientProvider adminClientProvider,
                                      long operationTimeoutMs) {
        this.vertx = vertx;
        this.podOps = podOps;
        this.adminClientProvider = adminClientProvider;
        this.operationTimeoutMs = operationTimeoutMs;
    }

    @Override
    public KafkaRoller kafkaRoller(Reconciliation reconciliation, int replicas,
                                   Secret clusterCaCertSecret,
                                   Secret coKeySecret,
                                   KafkaCluster kafkaCluster,
                                   String kafkaLogging,
                                   Function<Pod, List<String>> rollPodAndLogReason) {
        String cluster = reconciliation.name();
        var admin = adminClientProvider.createAdminClient(KafkaCluster.headlessServiceName(cluster),
                clusterCaCertSecret, coKeySecret, "cluster-operator");
        var ka = new KafkaAvailability(reconciliation, vertx, admin);
        return new KafkaRoller(reconciliation,
                vertx,
                podOps,
                operationTimeoutMs,
                reconciliation.namespace(),
                cluster,
                replicas,
                clusterCaCertSecret,
                coKeySecret,
                ka,
                kafkaCluster.getBrokersConfiguration(),
                kafkaLogging,
                kafkaCluster.getKafkaVersion(),
                true,
                rollPodAndLogReason);
    }
}
