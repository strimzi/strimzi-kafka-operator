/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeBuilder;
import io.fabric8.kubernetes.api.model.NodeList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.operator.resource.concurrent.AbstractNonNamespacedResourceOperator;
import io.strimzi.operator.common.operator.resource.concurrent.AbstractNonNamespacedResourceOperatorIT;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class NodeOperatorIT extends AbstractNonNamespacedResourceOperatorIT<KubernetesClient,
        Node, NodeList, Resource<Node>> {

    @Override
    public AbstractNonNamespacedResourceOperator<KubernetesClient,
            Node, NodeList, Resource<Node>> operator() {
        return new NodeOperator(asyncExecutor, client);
    }

    @Override
    public Node getOriginal()  {
        return new NodeBuilder()
                .withNewMetadata()
                    .withName(resourceName)
                    .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
                .withNewSpec()
                    .withUnschedulable(true)
                    .withPodCIDR("172.16.3.0/24")
                .endSpec()
                .build();
    }

    @Override
    public Node getModified()  {
        return new NodeBuilder()
                .withNewMetadata()
                    .withName(resourceName)
                    .withLabels(singletonMap("bar", "foo"))
                .endMetadata()
                .withNewSpec()
                    .withUnschedulable(true)
                    .withPodCIDR("172.16.3.0/24")
                .endSpec()
                .build();
    }

    @Override
    public void assertResources(Node expected, Node actual)   {
        assertThat(actual.getMetadata().getName(), is(expected.getMetadata().getName()));
        assertThat(actual.getMetadata().getLabels(), is(expected.getMetadata().getLabels()));
        assertThat(actual.getSpec().getUnschedulable(), is(expected.getSpec().getUnschedulable()));
    }
}
