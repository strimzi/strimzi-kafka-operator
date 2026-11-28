/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccountList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.ServiceAccountResource;
import io.strimzi.operator.common.operator.resource.kubernetes.AbstractNamespacedResourceOperator;
import io.strimzi.operator.common.operator.resource.kubernetes.AbstractNamespacedResourceOperatorTest;

import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.when;

public class ServiceAccountOperatorTest extends AbstractNamespacedResourceOperatorTest<KubernetesClient, ServiceAccount, ServiceAccountList, ServiceAccountResource> {

    @Override
    protected boolean alwaysUsesServerSideApply() {
        return true;
    }

    @Override
    protected boolean supportsServerSideApply() {
        return alwaysUsesServerSideApply();
    }

    @Override
    protected Class<KubernetesClient> clientType() {
        return KubernetesClient.class;
    }

    @Override
    protected Class<? extends Resource> resourceType() {
        return ServiceAccountResource.class;
    }

    @Override
    protected ServiceAccount resource(String name) {
        return new ServiceAccountBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(NAMESPACE)
                    .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
            .build();
    }

    @Override
    protected ServiceAccount modifiedResource(String name) {
        return new ServiceAccountBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(NAMESPACE)
                    .withLabels(singletonMap("foo2", "bar2"))
                .endMetadata()
                .build();
    }

    @Override
    protected void mocker(KubernetesClient mockClient, MixedOperation<ServiceAccount, ServiceAccountList, ServiceAccountResource> op) {
        when(mockClient.serviceAccounts()).thenReturn(op);
    }

    @Override
    protected AbstractNamespacedResourceOperator<KubernetesClient, ServiceAccount, ServiceAccountList, ServiceAccountResource> createResourceOperations(KubernetesClient mockClient) {
        return new ServiceAccountOperator(asyncExecutor, mockClient);
    }
}
