/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.model.RestartReasons;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.function.Function;

/**
 * Specialization of {@link StatefulSetOperator} for StatefulSets of Kafka brokers
 */
public class KafkaSetOperator extends StatefulSetOperator {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaSetOperator.class);

    private final AdminClientProvider adminClientProvider;

    /**
     * Constructor
     *
     * @param vertx  The Vertx instance
     * @param client The Kubernetes client
     * @param operationTimeoutMs The timeout.
     * @param adminClientProvider A provider for the AdminClient.
     * @param metricsProvider - metrics provider needed by pod operator for publishing restart reasons
     */
    public KafkaSetOperator(Vertx vertx, KubernetesClient client, long operationTimeoutMs,
                            AdminClientProvider adminClientProvider, MetricsProvider metricsProvider) {
        super(vertx, client, operationTimeoutMs, metricsProvider);
        this.adminClientProvider = adminClientProvider;
    }

    @Override
    protected boolean shouldIncrementGeneration(Reconciliation reconciliation, StatefulSetDiff diff) {
        return !diff.isEmpty() && needsRollingUpdate(reconciliation, diff);
    }

    public static boolean needsRollingUpdate(Reconciliation reconciliation, StatefulSetDiff diff) {
        if (diff.changesLabels()) {
            LOGGER.debugCr(reconciliation, "Changed labels => needs rolling update");
            return true;
        }
        if (diff.changesSpecTemplate()) {
            LOGGER.debugCr(reconciliation, "Changed template spec => needs rolling update");
            return true;
        }
        if (diff.changesVolumeClaimTemplates()) {
            LOGGER.debugCr(reconciliation, "Changed volume claim template => needs rolling update");
            return true;
        }
        if (diff.changesVolumeSize()) {
            LOGGER.debugCr(reconciliation, "Changed size of the volume claim template => no need for rolling update");
            return false;
        }
        return false;
    }

    @Override
    public Future<Void> maybeRollingUpdate(Reconciliation reconciliation, StatefulSet sts, Function<Pod, RestartReasons> podNeedsRestart) {
        return maybeRollingUpdate(reconciliation, sts, podNeedsRestart, null, null);
    }

    @Override
    public Future<Void> maybeRollingUpdate(Reconciliation reconciliation, StatefulSet sts, Function<Pod, RestartReasons> podNeedsRestart,
                                           Secret clusterCaCertSecret, Secret coKeySecret) {
        throw new UnsupportedOperationException();
    }
}
