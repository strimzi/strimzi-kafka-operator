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
import io.fabric8.kubernetes.client.dsl.V1beta1PolicyAPIGroupDSL;
import io.vertx.core.Vertx;

import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PodDisruptionBudgetV1Beta1OperatorTest extends AbstractNamespacedResourceOperatorTest<KubernetesClient, PodDisruptionBudget, PodDisruptionBudgetList, Resource<PodDisruptionBudget>> {

    @Override
    protected void  mocker(KubernetesClient mockClient, MixedOperation op) {
        PolicyAPIGroupDSL mockPolicy = mock(PolicyAPIGroupDSL.class);
        V1beta1PolicyAPIGroupDSL mockV1beta1 = mock(V1beta1PolicyAPIGroupDSL.class);
        when(mockPolicy.v1beta1()).thenReturn(mockV1beta1);
        when(mockV1beta1.podDisruptionBudget()).thenReturn(op);
        when(mockClient.policy()).thenReturn(mockPolicy);
    }

    @Override
    protected AbstractNamespacedResourceOperator<KubernetesClient, PodDisruptionBudget, PodDisruptionBudgetList, Resource<PodDisruptionBudget>> createResourceOperations(Vertx vertx, KubernetesClient mockClient) {
        return new PodDisruptionBudgetV1Beta1Operator(vertx, mockClient);
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
    protected PodDisruptionBudget resource(String name) {
        return new PodDisruptionBudgetBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(NAMESPACE)
                    .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
                .withNewSpec()
                    .withNewMaxUnavailable(1)
                .endSpec()
                .build();
    }

    @Override
    protected PodDisruptionBudget modifiedResource(String name) {
        return new PodDisruptionBudgetBuilder(resource(name))
                .editSpec()
                    .withNewMaxUnavailable(2)
                .endSpec()
                .build();
    }
}
