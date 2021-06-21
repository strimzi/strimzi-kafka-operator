/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.ArrayOrObjectKafkaListeners;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.status.ConditionBuilder;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaCrdOperatorTest extends AbstractResourceOperatorTest<KubernetesClient, Kafka, KafkaList, Resource<Kafka>> {

    @Override
    protected Class<KubernetesClient> clientType() {
        return KubernetesClient.class;
    }

    @Override
    @SuppressWarnings("rawtypes")
    protected Class<Resource> resourceType() {
        return Resource.class;
    }

    @Override
    protected Kafka resource() {
        return new KafkaBuilder()
                .withApiVersion(Kafka.RESOURCE_GROUP + "/" + Kafka.V1BETA1)
                .withNewMetadata()
                    .withName(RESOURCE_NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(1)
                        .withListeners(new ArrayOrObjectKafkaListeners(List.of(new GenericKafkaListenerBuilder().build())))
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(1)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                .withNewStatus()
                    .addToConditions(new ConditionBuilder().withNewStatus("Ready").withNewMessage("Kafka is ready").build())
                .endStatus()
                .build();
    }

    @Override
    protected Kafka modifiedResource() {
        return new KafkaBuilder(resource())
                .editSpec()
                    .withNewEntityOperator()
                        .withNewTopicOperator()
                        .endTopicOperator()
                        .withNewUserOperator()
                        .endUserOperator()
                    .endEntityOperator()
                .endSpec()
                .build();
    }


    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected void mocker(KubernetesClient mockClient, MixedOperation op) {
        when(mockClient.customResources(any(), any())).thenReturn(op);
    }

    @Override
    protected CrdOperator<KubernetesClient, Kafka, KafkaList> createResourceOperations(Vertx vertx, KubernetesClient mockClient) {
        return new CrdOperator<>(vertx, mockClient, Kafka.class, KafkaList.class, Kafka.RESOURCE_KIND);
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testUpdateStatusAsync(VertxTestContext context) {
        Kafka resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.updateStatus(any())).thenReturn(resource);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        KubernetesClient mockClient = mock(KubernetesClient.class);
        mocker(mockClient, mockCms);

        createResourceOperations(vertx, mockClient)
            .updateStatusAsync(Reconciliation.DUMMY_RECONCILIATION, resource())
            .onComplete(context.succeeding(k -> context.completeNow()));
    }
}
