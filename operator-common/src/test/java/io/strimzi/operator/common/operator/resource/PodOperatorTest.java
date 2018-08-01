/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.strimzi.operator.common.model.Labels;

import io.fabric8.kubernetes.api.model.DoneablePod;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.client.server.mock.OpenShiftServer;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.Rule;
import org.junit.Test;

import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.mockito.Mockito.when;

public class PodOperatorTest extends
        AbtractReadyResourceOperatorTest<KubernetesClient, Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> {

    @Rule
    public OpenShiftServer server = new OpenShiftServer(false, true);

    @Test
    public void testCreateReadUpdate(TestContext context) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool", 10);
        KubernetesClient client = server.getKubernetesClient();
        PodOperator pr = new PodOperator(vertx, client);

        context.assertEquals(emptyList(), pr.list(NAMESPACE, Labels.EMPTY));

        Async async = context.async(1);
        pr.createOrUpdate(resource()).setHandler(createResult -> {
            context.assertTrue(createResult.succeeded());
            context.assertEquals(singletonList(RESOURCE_NAME), pr.list(NAMESPACE, Labels.EMPTY).stream()
                        .map(p -> p.getMetadata().getName())
                        .collect(Collectors.toList()));
            //Pod got = pr.get(NAMESPACE, RESOURCE_NAME);
            //context.assertNotNull(got);
            //context.assertNotNull(got.getMetadata());
            //context.assertEquals(RESOURCE_NAME, got.getMetadata().getName());
            context.assertFalse(pr.isReady(NAMESPACE, RESOURCE_NAME));
            /*pr.watch(NAMESPACE, RESOURCE_NAME, new Watcher<Pod>() {
                @Override
                public void eventReceived(Action action, Pod resource) {
                    if (action == Action.DELETED) {
                        context.assertEquals(RESOURCE_NAME, resource.getMetadata().getName());
                    } else {
                        context.fail();
                    }
                    async.countDown();
                }

                @Override
                public void onClose(KubernetesClientException cause) {

                }
            });*/
            /*Pod modified = resource();
            modified.getSpec().setHostname("bar");
            Async patchAsync = context.async();
            pr.patch(NAMESPACE, RESOURCE_NAME, modified, patchResult -> {
                context.assertTrue(patchResult.succeeded());
                patchAsync.complete();
            });
            patchAsync.await();*/
            pr.reconcile(NAMESPACE, RESOURCE_NAME, null).setHandler(deleteResult -> {
                context.assertTrue(deleteResult.succeeded());
                async.countDown();
            });

        });
    }

    @Override
    protected Class clientType() {
        return KubernetesClient.class;
    }

    @Override
    protected Class<? extends Resource> resourceType() {
        return Resource.class;
    }

    @Override
    protected Pod resource() {
        return new PodBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(RESOURCE_NAME)
                .endMetadata()
                .withNewSpec()
                    .withHostname("foo")
                .endSpec().build();
    }

    @Override
    protected void mocker(KubernetesClient client, MixedOperation op) {
        when(client.pods()).thenReturn(op);
    }

    @Override
    protected PodOperator createResourceOperations(Vertx vertx, KubernetesClient mockClient) {
        return new PodOperator(vertx, mockClient);
    }
}
