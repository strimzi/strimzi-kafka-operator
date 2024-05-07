/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeBuilder;
import io.fabric8.kubernetes.api.model.NodeList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Vertx;

import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.when;

public class NodeOperatorTest extends AbstractNonNamespacedResourceOperatorTest<KubernetesClient,
        Node, NodeList, Resource<Node>> {

    @Override
    protected void mocker(KubernetesClient mockClient, MixedOperation op) {
        when(mockClient.nodes()).thenReturn(op);
    }

    @Override
    protected AbstractNonNamespacedResourceOperator<KubernetesClient, Node, NodeList,
            Resource<Node>> createResourceOperations(
                    Vertx vertx, KubernetesClient mockClient) {
        return new NodeOperator(vertx, mockClient) {
            @Override
            protected long deleteTimeoutMs() {
                return 100;
            }
        };
    }

    @Override
    protected Class<KubernetesClient> clientType() {
        return KubernetesClient.class;
    }

    @Override
    protected Class<? extends Resource> resourceType() {
        return Resource.class;
    }

    @Override
    protected Node resource() {
        return new NodeBuilder()
                .withNewMetadata()
                    .withName(RESOURCE_NAME)
                    .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
                .build();
    }

    @Override
    protected Node modifiedResource() {
        return new NodeBuilder()
                .withNewMetadata()
                    .withName(RESOURCE_NAME)
                    .withLabels(singletonMap("foo2", "bar2"))
                .endMetadata()
                .build();
    }
}
