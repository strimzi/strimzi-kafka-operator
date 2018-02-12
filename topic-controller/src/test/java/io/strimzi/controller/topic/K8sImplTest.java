/*
 * Copyright 2018, Strimzi Authors.
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
package io.strimzi.controller.topic;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.ConfigMapListBuilder;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class K8sImplTest {

    private Vertx vertx = Vertx.vertx();

    @Test
    public void testList(TestContext context) {
        Async async = context.async();

        KubernetesClient mockClient = mock(KubernetesClient.class);
        MixedOperation<ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> mockConfigMaps = mock(MixedOperation.class);
        when(mockClient.configMaps()).thenReturn(mockConfigMaps);
        when(mockConfigMaps.withLabels(any())).thenReturn(mockConfigMaps);
        when(mockConfigMaps.inNamespace(any())).thenReturn(mockConfigMaps);
        when(mockConfigMaps.list()).thenReturn(new ConfigMapListBuilder()
                .addNewItem().withKind("ConfigMap")
                .withNewMetadata()
                .withName("unrelated")
                .withLabels(Collections.singletonMap("foo", "bar"))
                .endMetadata().withData(Collections.singletonMap("foo", "bar")).endItem()
                .addNewItem().endItem()
                .build());

        K8sImpl k8s = new K8sImpl(vertx, mockClient, new LabelPredicate("foo", "bar"), "default");

        k8s.listMaps(ar -> {
            List<ConfigMap> list = ar.result();
            context.assertFalse(list.isEmpty());
            async.complete();
        });
    }
}
