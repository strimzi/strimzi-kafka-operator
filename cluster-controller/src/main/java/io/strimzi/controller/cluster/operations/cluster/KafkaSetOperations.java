/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.cluster;

import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.controller.cluster.operations.resource.ReconcileResult;
import io.strimzi.controller.cluster.operations.resource.StatefulSetOperations;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.HashSet;

import static io.strimzi.controller.cluster.resources.KafkaCluster.*;
import static java.util.Arrays.asList;

/**
 * Specialization of {@link StatefulSetOperations} for StatefulSets of Kafka brokers
 */
public class KafkaSetOperations extends StatefulSetOperations<Boolean> {

    /**
     * Constructor
     *
     * @param vertx  The Vertx instance
     * @param client The Kubernetes client
     */
    public KafkaSetOperations(Vertx vertx, KubernetesClient client) {
        super(vertx, client);
    }

    @Override
    protected Future<ReconcileResult<Boolean>> internalPatch(String namespace, String name, StatefulSet current, StatefulSet desired) {
        boolean different = needsRollingUpdate(current, desired);
        return super.internalPatch(namespace, name, current, desired).map(r -> {
            if (r instanceof ReconcileResult.Patched) {
                return ReconcileResult.patched(different);
            } else {
                return r;
            }
        });
    }

    static boolean needsRollingUpdate(StatefulSet current, StatefulSet desired) {
        return Diffs.differingLabels(current, desired)
                    || Diffs.differingContainers(current, desired)
                    || Diffs.differingContainers(
                            current.getSpec().getTemplate().getSpec().getContainers().get(0),
                            desired.getSpec().getTemplate().getSpec().getContainers().get(0))
                    || Diffs.differingEnvironments(
                            current.getSpec().getTemplate().getSpec().getContainers().get(0),
                            desired.getSpec().getTemplate().getSpec().getContainers().get(0),
                        new HashSet(asList(KEY_KAFKA_ZOOKEEPER_CONNECT, KEY_KAFKA_DEFAULT_REPLICATION_FACTOR,
                                KEY_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR, KEY_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR))
                    );
    }
}
