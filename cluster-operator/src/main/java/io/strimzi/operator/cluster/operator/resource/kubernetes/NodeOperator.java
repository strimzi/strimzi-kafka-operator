/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.operator.resource.kubernetes.AbstractNonNamespacedResourceOperator;

import java.util.concurrent.Executor;

/**
 * Operator for managing nodes
 */
public class NodeOperator extends AbstractNonNamespacedResourceOperator<KubernetesClient,
        Node, NodeList, Resource<Node>> {
    /**
     * Constructor.
     *
     * @param asyncExecutor Executor to use for asynchronous subroutines
     * @param client The Kubernetes client.
     */
    public NodeOperator(Executor asyncExecutor, KubernetesClient client) {
        super(asyncExecutor, client, "Node");
    }

    @Override
    protected NonNamespaceOperation<Node, NodeList, Resource<Node>> operation() {
        return client.nodes();
    }
}
