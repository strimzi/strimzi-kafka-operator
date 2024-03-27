/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watcher;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaList;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.CrdOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

public class KafkaAssemblyOperatorNodePoolWatcherTest {
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
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("plain")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .build())
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
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.get(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(KAFKA);

        MockKafkaAssemblyOperator mockKao = new MockKafkaAssemblyOperator(null, supplier);
        mockKao.nodePoolEventHandler(Watcher.Action.ADDED, POOL);

        assertThat(mockKao.reconciliations.size(), is(1));
        assertThat(mockKao.reconciliations.get(0).kind(), is(Kafka.RESOURCE_KIND));
        assertThat(mockKao.reconciliations.get(0).name(), is(CLUSTER_NAME));
        assertThat(mockKao.reconciliations.get(0).namespace(), is(NAMESPACE));
    }

    @Test
    public void testEnqueueingResourceWithMatchingSelector() {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.get(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(KAFKA);

        MockKafkaAssemblyOperator mockKao = new MockKafkaAssemblyOperator(new LabelSelectorBuilder().withMatchLabels(Map.of("selector", "matching")).build(), supplier);
        mockKao.nodePoolEventHandler(Watcher.Action.ADDED, POOL);

        assertThat(mockKao.reconciliations.size(), is(1));
        assertThat(mockKao.reconciliations.get(0).kind(), is(Kafka.RESOURCE_KIND));
        assertThat(mockKao.reconciliations.get(0).name(), is(CLUSTER_NAME));
        assertThat(mockKao.reconciliations.get(0).namespace(), is(NAMESPACE));
    }

    @Test
    public void testEnqueueingResourceWithNonMatchingSelector()    {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.get(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(KAFKA);

        MockKafkaAssemblyOperator mockKao = new MockKafkaAssemblyOperator(new LabelSelectorBuilder().withMatchLabels(Map.of("selector", "not-matching")).build(), supplier);
        mockKao.nodePoolEventHandler(Watcher.Action.ADDED, POOL);

        assertThat(mockKao.reconciliations.size(), is(0));
    }

    @Test
    public void testEnqueueingResourceMissingKafka()    {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.get(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(null);

        MockKafkaAssemblyOperator mockKao = new MockKafkaAssemblyOperator(new LabelSelectorBuilder().withMatchLabels(Map.of("selector", "not-matching")).build(), supplier);
        mockKao.nodePoolEventHandler(Watcher.Action.ADDED, POOL);

        assertThat(mockKao.reconciliations.size(), is(0));
    }

    @Test
    public void testEnqueueingResourceWithMissingAnnotation()    {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "not-enabled"))
                .endMetadata()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.get(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(kafka);

        MockKafkaAssemblyOperator mockKao = new MockKafkaAssemblyOperator(null, supplier);
        mockKao.nodePoolEventHandler(Watcher.Action.ADDED, POOL);

        assertThat(mockKao.reconciliations.size(), is(0));
    }

    @Test
    public void testEnqueueingResourceMissingClusterLabel()    {
        KafkaNodePool pool = new KafkaNodePoolBuilder(POOL)
                .editMetadata()
                    .withLabels(Map.of())
                .endMetadata()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.get(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(KAFKA);

        MockKafkaAssemblyOperator mockKao = new MockKafkaAssemblyOperator(null, supplier);
        mockKao.nodePoolEventHandler(Watcher.Action.ADDED, pool);

        assertThat(mockKao.reconciliations.size(), is(0));
    }

    @Test
    public void testEnqueueingResourceWrongClusterLabel() {
        KafkaNodePool pool = new KafkaNodePoolBuilder(POOL)
                .editMetadata()
                    .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, "some-other-cluster"))
                .endMetadata()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.get(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(KAFKA);

        MockKafkaAssemblyOperator mockKao = new MockKafkaAssemblyOperator(null, supplier);
        mockKao.nodePoolEventHandler(Watcher.Action.ADDED, pool);

        assertThat(mockKao.reconciliations.size(), is(0));
    }

    static class MockKafkaAssemblyOperator extends KafkaAssemblyOperator  {
        private final LabelSelector selector;

        public List<Reconciliation> reconciliations = new ArrayList<>();

        public MockKafkaAssemblyOperator(LabelSelector selector, ResourceOperatorSupplier supplier) {
            super(Vertx.vertx(), null, null, null, supplier, ResourceUtils.dummyClusterOperatorConfig());
            this.selector = selector;
        }

        @Override
        public LabelSelector selector() {
            return selector;
        }

        @Override
        public void enqueueReconciliation(Reconciliation reconciliation) {
            reconciliations.add(reconciliation);
        }
    }
}
