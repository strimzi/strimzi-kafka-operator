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
import io.vertx.core.Vertx;

import static org.mockito.Mockito.when;


public class PodOperationsMockTest extends ResourceOperationsMockTest<KubernetesClient, Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> {

    /*@Rule
    public OpenShiftServer server = new OpenShiftServer(false, true);

    @Test
    public void testCrud(TestContext context) {
        KubernetesClient client = server.getKubernetesClient();
        PodResources pr = new PodResources(vertx, client);

        context.assertEquals(emptyList(), pr.list("test", emptyMap()));

        Async async = context.async();
        pr.create(resource(), ar -> {
            context.assertTrue(ar.succeeded());
            context.assertEquals(singletonList("mypod"), pr.list("test", emptyMap()).stream()
                        .map(p -> p.getMetadata().getName())
                        .collect(Collectors.toList()));
            //context.assertTrue(pr.isPodReady("test", "mypod"));
            pr.delete("test", "mypod", deleteResult -> {
                context.assertTrue(ar.succeeded());
                async.complete();
            });
        });
    }*/

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
        return new PodBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(NAME).endMetadata().build();
    }

    @Override
    protected void mocker(KubernetesClient client, MixedOperation op) {
        when(client.pods()).thenReturn(op);
    }

    @Override
    protected PodResources createResourceOperations(Vertx vertx, KubernetesClient mockClient) {
        return new PodResources(vertx, mockClient);
    }
}
