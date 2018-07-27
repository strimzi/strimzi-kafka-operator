/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.KafkaTopicList;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopicBuilder;
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
        MixedOperation<KafkaTopic, KafkaTopicList, TopicOperator.DeleteKafkaTopic, Resource<KafkaTopic, TopicOperator.DeleteKafkaTopic>> mockResources = mock(MixedOperation.class);
        when(mockClient.customResources(any(CustomResourceDefinition.class), any(Class.class), any(Class.class), any(Class.class))).thenReturn(mockResources);
        when(mockResources.withLabels(any())).thenReturn(mockResources);
        when(mockResources.inNamespace(any())).thenReturn(mockResources);
        when(mockResources.list()).thenAnswer(invocation -> {
            KafkaTopicList ktl = new KafkaTopicList();
            ktl.setItems(Collections.singletonList(new KafkaTopicBuilder()
                .withMetadata(new ObjectMetaBuilder()
                    .withName("unrelated")
                    .withLabels(Collections.singletonMap("foo", "bar")).build())
                .build()));
            return ktl;
        });

        K8sImpl k8s = new K8sImpl(vertx, mockClient, new LabelPredicate("foo", "bar"), "default");

        k8s.listMaps(ar -> {
            if (ar.failed()) {
                ar.cause().printStackTrace();
            }
            List<KafkaTopic> list = ar.result();
            context.assertFalse(list.isEmpty());
            async.complete();
        });
    }
}
