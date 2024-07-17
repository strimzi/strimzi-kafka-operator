/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DeploymentOperatorTest extends
        ScalableResourceOperatorTest<KubernetesClient, Deployment, DeploymentList, RollableScalableResource<Deployment>> {

    @Override
    protected Class<KubernetesClient> clientType() {
        return KubernetesClient.class;
    }

    @Override
    protected Class<RollableScalableResource> resourceType() {
        return RollableScalableResource.class;
    }

    @Override
    protected Deployment resource(String name) {
        return new DeploymentBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(name)
                    .addToAnnotations(Annotations.ANNO_DEP_KUBE_IO_REVISION, "test")
                .endMetadata()
                .withNewSpec()
                    .withNewStrategy()
                        .withType("RollingUpdate")
                    .endStrategy()
                .endSpec()
                .build();
    }

    @Override
    protected Deployment modifiedResource(String name) {
        return new DeploymentBuilder(resource(name))
                .editSpec()
                    .editStrategy()
                        .withType("Recreate")
                    .endStrategy()
                .endSpec()
                .build();
    }

    @Override
    protected void mocker(KubernetesClient mockClient, MixedOperation op) {
        AppsAPIGroupDSL mockExt = mock(AppsAPIGroupDSL.class);
        when(mockExt.deployments()).thenReturn(op);
        when(mockClient.apps()).thenReturn(mockExt);

    }

    @Override
    protected DeploymentOperator createResourceOperations(Vertx vertx, KubernetesClient mockClient) {
        return new DeploymentOperator(vertx, mockClient);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testSinglePodDeploymentRollingUpdate(VertxTestContext context)  {
        String depName = "my-dep";
        String podName = depName + "-123456";

        Pod pod = new PodBuilder()
                .withNewMetadata()
                    .withName(podName)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .build();

        // Mock Pod handling
        KubernetesResourceList mockPodResourceList = mock(KubernetesResourceList.class);
        when(mockPodResourceList.getItems()).thenReturn(List.of(pod));

        Resource mockPodResource = mock(Resource.class);
        when(mockPodResource.delete()).thenReturn(List.of());
        when(mockPodResource.withPropagationPolicy(any())).thenReturn(mockPodResource);
        when(mockPodResource.withGracePeriod(anyLong())).thenReturn(mockPodResource);
        when(mockPodResource.watch(any())).thenAnswer(invocation -> {
            Watcher watcher = invocation.getArgument(0);
            watcher.eventReceived(Watcher.Action.DELETED, pod);
            return (Watch) () -> { };
        });

        NonNamespaceOperation mockPodNonNamespaceOp = mock(NonNamespaceOperation.class);
        when(mockPodNonNamespaceOp.list(any())).thenReturn(mockPodResourceList);
        when(mockPodNonNamespaceOp.withLabels(any())).thenReturn(mockPodNonNamespaceOp);
        when(mockPodNonNamespaceOp.withName(eq(podName))).thenReturn(mockPodResource);

        MixedOperation mockPods = mock(MixedOperation.class);
        when(mockPods.inNamespace(eq(NAMESPACE))).thenReturn(mockPodNonNamespaceOp);

        // Mock Deployment handling
        Resource mockDeploymentResource = mock(resourceType());
        when(mockDeploymentResource.get()).thenReturn(new Deployment());
        when(mockDeploymentResource.isReady()).thenReturn(true);

        NonNamespaceOperation mockDeploymentNonNamespaceOp = mock(NonNamespaceOperation.class);
        when(mockDeploymentNonNamespaceOp.withName(eq(depName))).thenReturn(mockDeploymentResource);

        MixedOperation mockDeployments = mock(MixedOperation.class);
        when(mockDeployments.inNamespace(eq(NAMESPACE))).thenReturn(mockDeploymentNonNamespaceOp);

        AppsAPIGroupDSL mockApps = mock(AppsAPIGroupDSL.class);
        when(mockApps.deployments()).thenReturn(mockDeployments);

        // Mock Kube Client
        KubernetesClient mockClient = mock(KubernetesClient.class);
        when(mockClient.pods()).thenReturn(mockPods);
        when(mockClient.apps()).thenReturn(mockApps);

        DeploymentOperator op = new DeploymentOperator(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.singlePodDeploymentRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, NAMESPACE, depName, 5_000)
                .onComplete(context.succeeding(v -> {

                    verify(mockPodResource, times(1)).delete();

                    async.flag();
                }));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testSinglePodDeploymentRollingUpdateWithMissingPod(VertxTestContext context)  {
        String depName = "my-dep";
        String podName = depName + "-123456";

        // Mock Pod handling
        KubernetesResourceList mockPodResourceList = mock(KubernetesResourceList.class);
        when(mockPodResourceList.getItems()).thenReturn(List.of());

        Resource mockPodResource = mock(Resource.class);

        NonNamespaceOperation mockPodNonNamespaceOp = mock(NonNamespaceOperation.class);
        when(mockPodNonNamespaceOp.list(any())).thenReturn(mockPodResourceList);
        when(mockPodNonNamespaceOp.withLabels(any())).thenReturn(mockPodNonNamespaceOp);
        when(mockPodNonNamespaceOp.withName(eq(podName))).thenReturn(mockPodResource);

        MixedOperation mockPods = mock(MixedOperation.class);
        when(mockPods.inNamespace(eq(NAMESPACE))).thenReturn(mockPodNonNamespaceOp);

        // Mock Deployment handling
        Resource mockDeploymentResource = mock(resourceType());
        when(mockDeploymentResource.get()).thenReturn(new Deployment());
        when(mockDeploymentResource.isReady()).thenReturn(true);

        NonNamespaceOperation mockDeploymentNonNamespaceOp = mock(NonNamespaceOperation.class);
        when(mockDeploymentNonNamespaceOp.withName(eq(depName))).thenReturn(mockDeploymentResource);

        MixedOperation mockDeployments = mock(MixedOperation.class);
        when(mockDeployments.inNamespace(eq(NAMESPACE))).thenReturn(mockDeploymentNonNamespaceOp);

        AppsAPIGroupDSL mockApps = mock(AppsAPIGroupDSL.class);
        when(mockApps.deployments()).thenReturn(mockDeployments);

        // Mock Kube Client
        KubernetesClient mockClient = mock(KubernetesClient.class);
        when(mockClient.pods()).thenReturn(mockPods);
        when(mockClient.apps()).thenReturn(mockApps);

        DeploymentOperator op = new DeploymentOperator(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.singlePodDeploymentRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, NAMESPACE, depName, 5_000)
                .onComplete(context.succeeding(v -> {

                    verify(mockPodResource, never()).delete();

                    async.flag();
                }));
    }
}
