/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import java.util.List;
import java.util.function.Function;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.common.AdminClientProvider;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Specialization of {@link StatefulSetOperator} for StatefulSets of Kafka brokers
 */
public class KafkaSetOperator extends StatefulSetOperator {

    private static final Logger log = LogManager.getLogger(KafkaSetOperator.class);

    private final AdminClientProvider adminClientProvider;

    /**
     * Constructor
     *
     * @param vertx  The Vertx instance
     * @param client The Kubernetes client
     * @param operationTimeoutMs The timeout.
     * @param adminClientProvider A provider for the AdminClient.
     */
    public KafkaSetOperator(Vertx vertx, KubernetesClient client, long operationTimeoutMs,
                            AdminClientProvider adminClientProvider) {
        super(vertx, client, operationTimeoutMs);
        this.adminClientProvider = adminClientProvider;
    }

    @Override
    protected boolean shouldIncrementGeneration(StatefulSetDiff diff) {
        return !diff.isEmpty() && needsRollingUpdate(diff);
    }

    public static boolean needsRollingUpdate(StatefulSetDiff diff) {
        if (diff.changesLabels()) {
            log.debug("Changed labels => needs rolling update");
            return true;
        }
        if (diff.changesSpecTemplate()) {
            log.debug("Changed template spec => needs rolling update");
            return true;
        }
        if (diff.changesVolumeClaimTemplates()) {
            log.debug("Changed volume claim template => needs rolling update");
            return true;
        }
        if (diff.changesVolumeSize()) {
            log.debug("Changed size of the volume claim template => no need for rolling update");
            return false;
        }
        return false;
    }

    @Override
    public Future<Void> maybeRollingUpdate(StatefulSet sts, Function<Pod, List<String>> podNeedsRestart) {
        return maybeRollingUpdate(sts, podNeedsRestart, null, null);
    }

    @Override
    public Future<Void> maybeRollingUpdate(StatefulSet sts, Function<Pod, List<String>> podNeedsRestart,
                                           Secret clusterCaCertSecret, Secret coKeySecret) {
        throw new UnsupportedOperationException();
    }
}
