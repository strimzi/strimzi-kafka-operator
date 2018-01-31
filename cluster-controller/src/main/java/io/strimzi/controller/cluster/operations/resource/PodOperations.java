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
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.vertx.core.Vertx;

public class PodOperations extends AbstractOperations<KubernetesClient, Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> {

    public PodOperations(Vertx vertx, KubernetesClient client) {
        super(vertx, client, "Pods");
    }

    @Override
    protected MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> operation() {
        return client.pods();
    }

    public boolean isPodReady(String namespace, String name) {
        return operation().inNamespace(namespace).withName(name).isReady();
    }

    public Watch watch(String namespace, String name, Watcher<Pod> watcher) {
        return operation().inNamespace(namespace).withName(name).watch(watcher);
    }
}
