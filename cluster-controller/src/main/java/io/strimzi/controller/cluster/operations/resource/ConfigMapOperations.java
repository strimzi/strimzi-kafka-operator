/*
 * Copyright 2018 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.strimzi.controller.cluster.operations.resource;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Vertx;

/**
 * Operations for {@code ConfigMap}s.
 */
public class ConfigMapOperations extends AbstractOperations<KubernetesClient, ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> {
    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */
    public ConfigMapOperations(Vertx vertx, KubernetesClient client) {
        super(vertx, client, "ConfigMap");
    }

    @Override
    protected MixedOperation<ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> operation() {
        return client.configMaps();
    }
}
