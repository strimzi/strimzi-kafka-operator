/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.resource;

import io.fabric8.kubernetes.api.model.extensions.DoneableStatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetBuilder;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import org.junit.Ignore;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Ignore
public class StatefulSetOperationsMockTest extends ResourceOperationsMockTest<KubernetesClient, StatefulSet, StatefulSetList, DoneableStatefulSet, RollableScalableResource<StatefulSet, DoneableStatefulSet>> {

    @Override
    protected Class<KubernetesClient> clientType() {
        return KubernetesClient.class;
    }

    @Override
    protected Class<RollableScalableResource> resourceType() {
        return RollableScalableResource.class;
    }

    @Override
    protected StatefulSet resource() {
        return new StatefulSetBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(RESOURCE_NAME)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(3)
                .endSpec()
                .build();
    }

    @Override
    protected void mocker(KubernetesClient mockClient, MixedOperation op) {
        AppsAPIGroupDSL mockExt = mock(AppsAPIGroupDSL.class);
        when(mockExt.statefulSets()).thenReturn(op);
        when(mockClient.apps()).thenReturn(mockExt);
    }

    @Override
    protected StatefulSetOperations createResourceOperations(Vertx vertx, KubernetesClient mockClient) {
        return new StatefulSetOperations(vertx, mockClient) {
            @Override
            public Future<Void> readiness(String namespace, String name, long pollIntervalMs, long timeoutMs) {
                return Future.succeededFuture();
            }
            @Override
            protected Future<?> podReadiness(String namespace, StatefulSet desired, long pollInterval, long operationTimeoutMs) {
                return Future.succeededFuture();
            }
        };
    }

    @Override
    @Test
    public void createWhenExistsIsAPatch(TestContext context) {
        createWhenExistsIsAPatch(context, false);
    }

    // TODO Need to test that create waits for SS and pods
}
