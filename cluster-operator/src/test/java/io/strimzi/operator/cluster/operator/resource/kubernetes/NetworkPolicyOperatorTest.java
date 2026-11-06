/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NetworkAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.PatchContext;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.kubernetes.AbstractNamespacedResourceOperator;
import io.strimzi.operator.common.operator.resource.kubernetes.AbstractNamespacedResourceOperatorTest;
import org.junit.jupiter.api.Test;

import java.util.List;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NetworkPolicyOperatorTest extends AbstractNamespacedResourceOperatorTest<KubernetesClient, NetworkPolicy, NetworkPolicyList, Resource<NetworkPolicy>> {
    @Override
    protected Class<KubernetesClient> clientType() {
        return KubernetesClient.class;
    }

    @Override
    protected Class<? extends Resource> resourceType() {
        return Resource.class;
    }

    @Override
    protected NetworkPolicy resource(String name) {
        return new NetworkPolicyBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(NAMESPACE)
                    .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
                .withNewSpec()
                    .withNewPodSelector()
                        .withMatchLabels(singletonMap("app", "my-app"))
                    .endPodSelector()
                .endSpec()
                .build();
    }

    @Override
    protected NetworkPolicy modifiedResource(String name) {
        return new NetworkPolicyBuilder(resource(name))
                .editMetadata()
                    .withLabels(singletonMap("foo2", "bar2"))
                .endMetadata()
                .build();
    }

    @Override
    protected void mocker(KubernetesClient mockClient, MixedOperation<NetworkPolicy, NetworkPolicyList, Resource<NetworkPolicy>> op) {
        NetworkAPIGroupDSL network = mock(NetworkAPIGroupDSL.class);
        when(network.networkPolicies()).thenReturn(op);
        when(mockClient.network()).thenReturn(network);
    }

    @Override
    protected AbstractNamespacedResourceOperator<KubernetesClient, NetworkPolicy, NetworkPolicyList, Resource<NetworkPolicy>> createResourceOperations(KubernetesClient mockClient) {
        return new NetworkPolicyOperator(asyncExecutor, mockClient);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testFinalizersSetByOtherControllersAreKept() {
        NetworkPolicy desired = resource("my-np");
        NetworkPolicy currentWithFinalizer = new NetworkPolicyBuilder(desired)
                .editMetadata()
                    .addToFinalizers("networking.k8s.aws/resources")
                .endMetadata()
                .build();
        NetworkPolicy desiredWithOtherSpec = new NetworkPolicyBuilder(desired)
                .editSpec()
                    .editPodSelector()
                        .withMatchLabels(singletonMap("app", "other-app"))
                    .endPodSelector()
                .endSpec()
                .build();

        Resource<NetworkPolicy> resourceOp = mock(Resource.class);
        when(resourceOp.get()).thenReturn(currentWithFinalizer);
        when(resourceOp.patch(any(PatchContext.class), any(NetworkPolicy.class))).thenAnswer(i -> new NetworkPolicyBuilder((NetworkPolicy) i.getArgument(1))
                .editMetadata()
                    .withResourceVersion("new-version")
                .endMetadata()
                .build());
        NonNamespaceOperation<NetworkPolicy, NetworkPolicyList, Resource<NetworkPolicy>> nsOp = mock(NonNamespaceOperation.class);
        when(nsOp.withName(anyString())).thenReturn(resourceOp);
        MixedOperation<NetworkPolicy, NetworkPolicyList, Resource<NetworkPolicy>> npOp = mock(MixedOperation.class);
        when(npOp.inNamespace(anyString())).thenReturn(nsOp);
        KubernetesClient mockClient = mock(KubernetesClient.class);
        mocker(mockClient, npOp);
        NetworkPolicyOperator op = new NetworkPolicyOperator(asyncExecutor, mockClient);

        assertThat(op.reconcile(Reconciliation.DUMMY_RECONCILIATION, NAMESPACE, "my-np", desired).toCompletableFuture().join(), is(ReconcileResult.noop(currentWithFinalizer)));
        verify(resourceOp, never()).patch(any(PatchContext.class), any(NetworkPolicy.class));

        ReconcileResult<NetworkPolicy> patched = op.reconcile(Reconciliation.DUMMY_RECONCILIATION, NAMESPACE, "my-np", desiredWithOtherSpec).toCompletableFuture().join();
        assertThat(patched, is(instanceOf(ReconcileResult.Patched.class)));
        // The patch is computed against the desired resource, so it must carry the live finalizers or it removes them
        assertThat(patched.resource().getMetadata().getFinalizers(), is(List.of("networking.k8s.aws/resources")));
    }
}
