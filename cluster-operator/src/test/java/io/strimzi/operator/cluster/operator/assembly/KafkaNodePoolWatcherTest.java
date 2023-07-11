/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watcher;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.vertx.core.Future;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockMakers;

import java.util.Map;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class KafkaNodePoolWatcherTest {
    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final Kafka KAFKA = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(NAMESPACE)
                    .withLabels(Map.of("selector", "matching"))
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled"))
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("plain")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .build())
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                .build();

    private final static KafkaNodePool POOL = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("my-pool")
                .withNamespace(NAMESPACE)
                .withGeneration(1L)
                .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME))
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.BROKER)
                .withResources(new ResourceRequirementsBuilder().withRequests(Map.of("cpu", new Quantity("4"))).build())
            .endSpec()
            .build();

    @Test
    public void testEnqueueingResource()    {
        KafkaAssemblyOperator mockKao = mock(KafkaAssemblyOperator.class, withSettings().mockMaker(MockMakers.INLINE));
        when(mockKao.kind()).thenReturn("Kafka");
        ArgumentCaptor<Reconciliation> reconciliationCaptor = ArgumentCaptor.forClass(Reconciliation.class);
        when(mockKao.reconcile(reconciliationCaptor.capture())).thenReturn(Future.succeededFuture());

        @SuppressWarnings("unchecked")
        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = mock(CrdOperator.class);
        when(mockKafkaOps.get(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(KAFKA);

        KafkaNodePoolWatcher watcher = new KafkaNodePoolWatcher(NAMESPACE, Optional.empty(), mockKao, mockKafkaOps, null);
        watcher.eventReceived(Watcher.Action.ADDED, POOL);

        assertThat(reconciliationCaptor.getAllValues().size(), is(1));
        assertThat(reconciliationCaptor.getValue().kind(), is(Kafka.RESOURCE_KIND));
        assertThat(reconciliationCaptor.getValue().name(), is(CLUSTER_NAME));
        assertThat(reconciliationCaptor.getValue().namespace(), is(NAMESPACE));
    }

    @Test
    public void testEnqueueingResourceWithMatchingSelector()    {
        KafkaAssemblyOperator mockKao = mock(KafkaAssemblyOperator.class, withSettings().mockMaker(MockMakers.INLINE));
        when(mockKao.kind()).thenReturn("Kafka");
        ArgumentCaptor<Reconciliation> reconciliationCaptor = ArgumentCaptor.forClass(Reconciliation.class);
        when(mockKao.reconcile(reconciliationCaptor.capture())).thenReturn(Future.succeededFuture());

        @SuppressWarnings("unchecked")
        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = mock(CrdOperator.class);
        when(mockKafkaOps.get(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(KAFKA);

        KafkaNodePoolWatcher watcher = new KafkaNodePoolWatcher(NAMESPACE, Optional.of(new LabelSelectorBuilder().withMatchLabels(Map.of("selector", "matching")).build()), mockKao, mockKafkaOps, null);
        watcher.eventReceived(Watcher.Action.ADDED, POOL);

        assertThat(reconciliationCaptor.getAllValues().size(), is(1));
        assertThat(reconciliationCaptor.getValue().kind(), is(Kafka.RESOURCE_KIND));
        assertThat(reconciliationCaptor.getValue().name(), is(CLUSTER_NAME));
        assertThat(reconciliationCaptor.getValue().namespace(), is(NAMESPACE));
    }

    @Test
    public void testEnqueueingResourceWithNonMatchingSelector()    {
        KafkaAssemblyOperator mockKao = mock(KafkaAssemblyOperator.class, withSettings().mockMaker(MockMakers.INLINE));
        when(mockKao.kind()).thenReturn("Kafka");
        when(mockKao.reconcile(any(Reconciliation.class))).thenReturn(Future.succeededFuture());

        @SuppressWarnings("unchecked")
        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = mock(CrdOperator.class);
        when(mockKafkaOps.get(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(KAFKA);

        KafkaNodePoolWatcher watcher = new KafkaNodePoolWatcher(NAMESPACE, Optional.of(new LabelSelectorBuilder().withMatchLabels(Map.of("selector", "not-matching")).build()), mockKao, mockKafkaOps, null);
        watcher.eventReceived(Watcher.Action.ADDED, POOL);

        verify(mockKao, never()).reconcile(any(Reconciliation.class));
    }

    @Test
    public void testEnqueueingResourceMissingKafka()    {
        KafkaAssemblyOperator mockKao = mock(KafkaAssemblyOperator.class, withSettings().mockMaker(MockMakers.INLINE));
        when(mockKao.kind()).thenReturn("Kafka");
        when(mockKao.reconcile(any(Reconciliation.class))).thenReturn(Future.succeededFuture());

        @SuppressWarnings("unchecked")
        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = mock(CrdOperator.class);
        when(mockKafkaOps.get(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(null);

        KafkaNodePoolWatcher watcher = new KafkaNodePoolWatcher(NAMESPACE, Optional.of(new LabelSelectorBuilder().withMatchLabels(Map.of("selector", "matching")).build()), mockKao, mockKafkaOps, null);
        watcher.eventReceived(Watcher.Action.ADDED, POOL);

        verify(mockKao, never()).reconcile(any(Reconciliation.class));
    }

    @Test
    public void testEnqueueingResourceWithMissingAnnotation()    {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "not-enabled"))
                .endMetadata()
                .build();

        KafkaAssemblyOperator mockKao = mock(KafkaAssemblyOperator.class, withSettings().mockMaker(MockMakers.INLINE));
        when(mockKao.kind()).thenReturn("Kafka");
        when(mockKao.reconcile(any(Reconciliation.class))).thenReturn(Future.succeededFuture());

        @SuppressWarnings("unchecked")
        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = mock(CrdOperator.class);
        when(mockKafkaOps.get(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(kafka);

        KafkaNodePoolWatcher watcher = new KafkaNodePoolWatcher(NAMESPACE, Optional.empty(), mockKao, mockKafkaOps, null);
        watcher.eventReceived(Watcher.Action.ADDED, POOL);

        verify(mockKao, never()).reconcile(any(Reconciliation.class));
    }

    @Test
    public void testEnqueueingResourceMissingClusterLabel()    {
        KafkaNodePool pool = new KafkaNodePoolBuilder(POOL)
                .editMetadata()
                    .withLabels(Map.of())
                .endMetadata()
                .build();

        KafkaAssemblyOperator mockKao = mock(KafkaAssemblyOperator.class, withSettings().mockMaker(MockMakers.INLINE));
        when(mockKao.kind()).thenReturn("Kafka");
        when(mockKao.reconcile(any(Reconciliation.class))).thenReturn(Future.succeededFuture());

        @SuppressWarnings("unchecked")
        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = mock(CrdOperator.class);
        when(mockKafkaOps.get(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(KAFKA);

        KafkaNodePoolWatcher watcher = new KafkaNodePoolWatcher(NAMESPACE, Optional.of(new LabelSelectorBuilder().withMatchLabels(Map.of("selector", "not-matching")).build()), mockKao, mockKafkaOps, null);
        watcher.eventReceived(Watcher.Action.ADDED, pool);

        verify(mockKao, never()).reconcile(any(Reconciliation.class));
    }

    @Test
    public void testEnqueueingResourceWrongClusterLabel()    {
        KafkaNodePool pool = new KafkaNodePoolBuilder(POOL)
                .editMetadata()
                    .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, "some-other-cluster"))
                .endMetadata()
                .build();

        KafkaAssemblyOperator mockKao = mock(KafkaAssemblyOperator.class, withSettings().mockMaker(MockMakers.INLINE));
        when(mockKao.kind()).thenReturn("Kafka");
        when(mockKao.reconcile(any(Reconciliation.class))).thenReturn(Future.succeededFuture());

        @SuppressWarnings("unchecked")
        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = mock(CrdOperator.class);
        when(mockKafkaOps.get(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(KAFKA);

        KafkaNodePoolWatcher watcher = new KafkaNodePoolWatcher(NAMESPACE, Optional.of(new LabelSelectorBuilder().withMatchLabels(Map.of("selector", "not-matching")).build()), mockKao, mockKafkaOps, null);
        watcher.eventReceived(Watcher.Action.ADDED, pool);

        verify(mockKao, never()).reconcile(any(Reconciliation.class));
    }
}
