/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.ConfigMapListBuilder;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.model.Topic;
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
            List<Topic> list = ar.result();
            context.assertFalse(list.isEmpty());
            async.complete();
        });
    }
}
