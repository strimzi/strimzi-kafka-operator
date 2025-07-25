/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccountList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.ServiceAccountResource;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.extension.ExtendWith;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(VertxExtension.class)
public class ServiceAccountOperatorServerSideApplyIT extends AbstractNamespacedResourceOperatorServerSideApplyIT<KubernetesClient, ServiceAccount, ServiceAccountList, ServiceAccountResource> {
    @Override
    protected AbstractNamespacedResourceOperator<KubernetesClient, ServiceAccount, ServiceAccountList, ServiceAccountResource> operator() {
        return new ServiceAccountOperator(vertx, client, true);
    }

    @Override
    protected ServiceAccount getOriginal()  {
        return new ServiceAccountBuilder()
            .withNewMetadata()
                .withName(resourceName)
                .withNamespace(namespace)
                .withLabels(singletonMap("foo", "bar"))
            .endMetadata()
            .build();
    }

    @Override
    protected ServiceAccount getModified() {
        return new ServiceAccountBuilder()
            .withNewMetadata()
                .withName(resourceName)
                .withNamespace(namespace)
                .withLabels(singletonMap("foo2", "bar2"))
            .endMetadata()
            .build();
    }

    @Override
    protected void assertResources(VertxTestContext context, ServiceAccount expected, ServiceAccount actual)   {
        context.verify(() -> assertThat(actual.getMetadata().getName(), is(expected.getMetadata().getName())));
        context.verify(() -> assertThat(actual.getMetadata().getNamespace(), is(expected.getMetadata().getNamespace())));
        context.verify(() -> assertThat(actual.getMetadata().getLabels(), is(expected.getMetadata().getLabels())));
    }
}
