/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.strimzi.controller.cluster.operations.resource;

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
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.mockito.Mockito.when;

public class PodOperationsMockTest extends ResourceOperationsMockTest<KubernetesClient, Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> {

    @Rule
    public OpenShiftServer server = new OpenShiftServer(false, true);

    @Test
    public void testCreateReadUpdate(TestContext context) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool", 10);
        KubernetesClient client = server.getKubernetesClient();
        PodOperations pr = new PodOperations(vertx, client);

        context.assertEquals(emptyList(), pr.list(NAMESPACE, emptyMap()));

        Async async = context.async(1);
        pr.create(resource()).setHandler(createResult -> {
            context.assertTrue(createResult.succeeded());
            context.assertEquals(singletonList(RESOURCE_NAME), pr.list(NAMESPACE, emptyMap()).stream()
                        .map(p -> p.getMetadata().getName())
                        .collect(Collectors.toList()));
            //Pod got = pr.get(NAMESPACE, RESOURCE_NAME);
            //context.assertNotNull(got);
            //context.assertNotNull(got.getMetadata());
            //context.assertEquals(RESOURCE_NAME, got.getMetadata().getName());
            context.assertFalse(pr.isPodReady(NAMESPACE, RESOURCE_NAME));
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
            pr.delete(NAMESPACE, RESOURCE_NAME).setHandler(deleteResult -> {
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
    protected PodOperations createResourceOperations(Vertx vertx, KubernetesClient mockClient) {
        return new PodOperations(vertx, mockClient);
    }
}
