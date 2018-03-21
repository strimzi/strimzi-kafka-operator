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

import java.util.Collections;

import static io.strimzi.controller.cluster.resources.ZookeeperCluster.KEY_ZOOKEEPER_METRICS_ENABLED;

/**
 * Specialization of {@link StatefulSetOperations} for StatefulSets of Zookeeper nodes
 */
public class ZookeeperSetOperations extends StatefulSetOperations<Boolean> {

    /**
     * Constructor
     *
     * @param vertx  The Vertx instance
     * @param client The Kubernetes client
     */
    public ZookeeperSetOperations(Vertx vertx, KubernetesClient client) {
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
        // Because for ZK the brokers know about each other via the config, and rescaling requires a rolling update
        return Diffs.differingScale(current, desired)
                    || Diffs.differingLabels(current, desired)
                    || Diffs.differingContainers(current, desired)
                    || Diffs.differingContainers(
                            current.getSpec().getTemplate().getSpec().getContainers().get(0),
                            desired.getSpec().getTemplate().getSpec().getContainers().get(0))
                    || Diffs.differingEnvironments(
                            current.getSpec().getTemplate().getSpec().getContainers().get(0),
                            desired.getSpec().getTemplate().getSpec().getContainers().get(0),
                        Collections.singleton(KEY_ZOOKEEPER_METRICS_ENABLED));
    }
}
