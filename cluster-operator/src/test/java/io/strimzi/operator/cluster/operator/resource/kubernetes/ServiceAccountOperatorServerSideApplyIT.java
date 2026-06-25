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
import io.strimzi.operator.common.operator.resource.concurrent.AbstractNamespacedResourceOperator;
import io.strimzi.operator.common.operator.resource.concurrent.AbstractNamespacedResourceOperatorServerSideApplyIT;

import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ServiceAccountOperatorServerSideApplyIT extends AbstractNamespacedResourceOperatorServerSideApplyIT<KubernetesClient, ServiceAccount, ServiceAccountList, ServiceAccountResource> {
    @Override
    public AbstractNamespacedResourceOperator<KubernetesClient, ServiceAccount, ServiceAccountList, ServiceAccountResource> operator() {
        return new ServiceAccountOperator(asyncExecutor, client, true);
    }

    @Override
    public ServiceAccount getOriginal()  {
        return new ServiceAccountBuilder()
            .withNewMetadata()
                .withName(resourceName)
                .withNamespace(namespace)
                .withLabels(singletonMap("foo", "bar"))
            .endMetadata()
            .build();
    }

    @Override
    public ServiceAccount getModified() {
        return new ServiceAccountBuilder()
            .withNewMetadata()
                .withName(resourceName)
                .withNamespace(namespace)
                .withLabels(Map.of("foo", "bar", "foo2", "bar2"))
            .endMetadata()
            .withAutomountServiceAccountToken()
            .build();
    }

    @Override
    public ServiceAccount getNonConflicting() {
        return new ServiceAccountBuilder()
            .withNewMetadata()
                .withName(resourceName)
                .withNamespace(namespace)
                .withAnnotations(singletonMap("my-annotation2", "my-value2"))
            .endMetadata()
            .withAutomountServiceAccountToken(false)
            .build();
    }

    @Override
    public ServiceAccount getConflicting() {
        return new ServiceAccountBuilder()
            .withNewMetadata()
                .withName(resourceName)
                .withNamespace(namespace)
                .withLabels(singletonMap("foo", "bar"))
            .endMetadata()
            .withAutomountServiceAccountToken(false)
            .build();
    }

    @Override
    public void assertResources(ServiceAccount expected, ServiceAccount actual)   {
        assertThat(actual.getMetadata().getName(), is(expected.getMetadata().getName()));
        assertThat(actual.getMetadata().getNamespace(), is(expected.getMetadata().getNamespace()));
        assertThat(actual.getMetadata().getLabels(), is(expected.getMetadata().getLabels()));
        assertThat(actual.getAutomountServiceAccountToken(), is(expected.getAutomountServiceAccountToken()));
    }
}
