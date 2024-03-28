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
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.extension.ExtendWith;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(VertxExtension.class)
public class NodeOperatorIT extends AbstractNonNamespacedResourceOperatorIT<KubernetesClient,
        Node, NodeList, Resource<Node>> {

    @Override
    protected AbstractNonNamespacedResourceOperator<KubernetesClient,
            Node, NodeList, Resource<Node>> operator() {
        return new NodeOperator(vertx, client);
    }

    @Override
    protected Node getOriginal()  {
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
    protected Node getModified()  {
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
    protected void assertResources(VertxTestContext context, Node expected, Node actual)   {
        context.verify(() -> assertThat(actual.getMetadata().getName(), is(expected.getMetadata().getName())));
        context.verify(() -> assertThat(actual.getMetadata().getLabels(), is(expected.getMetadata().getLabels())));
        context.verify(() -> assertThat(actual.getSpec().getUnschedulable(), is(expected.getSpec().getUnschedulable())));
    }
}
