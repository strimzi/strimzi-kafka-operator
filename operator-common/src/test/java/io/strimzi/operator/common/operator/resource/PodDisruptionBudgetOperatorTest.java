/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudgetBuilder;
import io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudgetList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.PolicyAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Vertx;

import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PodDisruptionBudgetOperatorTest extends AbstractResourceOperatorTest<KubernetesClient, PodDisruptionBudget, PodDisruptionBudgetList, Resource<PodDisruptionBudget>> {

    @Override
    protected void  mocker(KubernetesClient mockClient, MixedOperation op) {
        PolicyAPIGroupDSL mockPolicy = mock(PolicyAPIGroupDSL.class);
        when(mockPolicy.v1beta1().podDisruptionBudget()).thenReturn(op);
        when(mockClient.policy()).thenReturn(mockPolicy);
    }

    @Override
    protected AbstractResourceOperator<KubernetesClient, PodDisruptionBudget, PodDisruptionBudgetList, Resource<PodDisruptionBudget>> createResourceOperations(Vertx vertx, KubernetesClient mockClient) {
        return new PodDisruptionBudgetOperator(vertx, mockClient);
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
    protected PodDisruptionBudget resource() {
        return new PodDisruptionBudgetBuilder()
                .withNewMetadata()
                    .withName(RESOURCE_NAME)
                    .withNamespace(NAMESPACE)
                    .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
                .withNewSpec()
                    .withNewMaxUnavailable(1)
                .endSpec()
                .build();
    }

    @Override
    protected PodDisruptionBudget modifiedResource() {
        return new PodDisruptionBudgetBuilder(resource())
                .editSpec()
                    .withNewMaxUnavailable(2)
                .endSpec()
                .build();
    }
}
