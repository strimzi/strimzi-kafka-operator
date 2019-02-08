/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaAssemblyList;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.model.JbodStorageBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.PersistentClaimStorage;
import io.strimzi.api.kafka.model.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.SingleVolumeStorage;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.ResourceType;
import io.strimzi.operator.common.operator.MockCertManager;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

@RunWith(VertxUnitRunner.class)
public class JbodStorageTest {

    private static final String NAMESPACE = "test-jbod-storage";
    private static final String NAME = "my-kafka";
    private static final KafkaVersion.Lookup VERSIONS = new KafkaVersion.Lookup(new StringReader(
            "2.0.0 default 2.0 2.0 1234567890abcdef"),
            singletonMap("2.0.0", "strimzi/kafka:latest-kafka-2.0.0"),
            emptyMap(), emptyMap(), emptyMap()) { };

    private Vertx vertx;
    private Kafka kafka;
    private KubernetesClient mockClient;
    private KafkaAssemblyOperator kao;

    private List<SingleVolumeStorage> volumes;

    private void init() {

        this.volumes = new ArrayList<>(2);

        volumes.add(new PersistentClaimStorageBuilder()
                .withId(0)
                .withDeleteClaim(true)
                .withSize("100Gi").build());
        volumes.add(new PersistentClaimStorageBuilder()
                .withId(1)
                .withDeleteClaim(false)
                .withSize("100Gi").build());

        this.kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(NAME)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withNewJbodStorage()
                            .withVolumes(volumes)
                        .endJbodStorage()
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(1)
                    .endZookeeper()
                .endSpec()
                .build();

        // setting up the Kafka CRD
        CustomResourceDefinition kafkaAssemblyCrd = Crds.kafka();

        // setting up a mock Kubernetes client
        this.mockClient = new MockKube()
                .withCustomResourceDefinition(kafkaAssemblyCrd, Kafka.class, KafkaAssemblyList.class, DoneableKafka.class)
                .end()
                .build();

        Crds.kafkaOperation(this.mockClient).inNamespace(NAMESPACE).withName(NAME).create(this.kafka);

        this.vertx = Vertx.vertx();

        // creating the Kafka operator
        ResourceOperatorSupplier ros =
                new ResourceOperatorSupplier(this.vertx, this.mockClient, false, 60_000L);

        this.kao = new KafkaAssemblyOperator(this.vertx, false, 2_000, new MockCertManager(), ros, VERSIONS, null);
    }

    @Test
    public void testCreatePersistentVolumeClaims(TestContext context) {

        this.init();

        Async async = context.async();
        this.kao.reconcileAssembly(new Reconciliation("test-trigger", ResourceType.KAFKA, NAMESPACE, NAME), ar -> {
            context.assertTrue(ar.succeeded());

            List<PersistentVolumeClaim> pvcs = getPvcs(NAMESPACE, NAME);

            for (int i = 0; i < this.kafka.getSpec().getKafka().getReplicas(); i++) {
                int podId = i;
                for (SingleVolumeStorage volume : this.volumes) {
                    if (volume instanceof PersistentClaimStorage) {
                        context.assertTrue(pvcs.stream().anyMatch(pvc -> {
                            String pvcName = ModelUtils.getVolumePrefix(volume.getId()) + "-"
                                    + KafkaCluster.kafkaPodName(NAME, podId);
                            boolean isDeleteClaim = ((PersistentClaimStorage) volume).isDeleteClaim();

                            boolean namesMatch = pvc.getMetadata().getName().equals(pvcName);
                            return namesMatch && Annotations.booleanAnnotation(pvc, AbstractModel.ANNO_STRIMZI_IO_DELETE_CLAIM,
                                    false, AbstractModel.ANNO_CO_STRIMZI_IO_DELETE_CLAIM) == isDeleteClaim;
                        }));
                    }
                }
            }
            async.complete();
        });
    }

    @Test
    public void testAddVolumeToJbod(TestContext context) {

        this.init();

        // first reconcile for cluster creation
        Async createAsync = context.async();
        this.kao.reconcileAssembly(new Reconciliation("test-trigger", ResourceType.KAFKA, NAMESPACE, NAME), ar -> {
            context.assertTrue(ar.succeeded());
            createAsync.complete();
        });
        createAsync.await();

        Set<String> expectedPvcs = expectedPvcs();

        // trying to add a new volume to the JBOD storage
        volumes.add(new PersistentClaimStorageBuilder()
                .withId(2)
                .withDeleteClaim(false)
                .withSize("100Gi").build());

        Kafka changedKafka = new KafkaBuilder(this.kafka)
                .editSpec()
                    .editKafka()
                        .withStorage(new JbodStorageBuilder().withVolumes(volumes).build())
                    .endKafka()
                .endSpec()
                .build();

        Crds.kafkaOperation(mockClient).inNamespace(NAMESPACE).withName(NAME).patch(changedKafka);

        Async updateAsync = context.async();
        this.kao.reconcileAssembly(new Reconciliation("test-trigger", ResourceType.KAFKA, NAMESPACE, NAME), ar -> {
            context.assertTrue(ar.succeeded());
            List<PersistentVolumeClaim> pvcs = getPvcs(NAMESPACE, NAME);
            Set<String> pvcsNames = pvcs.stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toSet());
            context.assertEquals(expectedPvcs, pvcsNames);
            updateAsync.complete();
        });
    }

    @Test
    public void testRemoveVolumeFromJbod(TestContext context) {

        this.init();

        // first reconcile for cluster creation
        Async createAsync = context.async();
        this.kao.reconcileAssembly(new Reconciliation("test-trigger", ResourceType.KAFKA, NAMESPACE, NAME), ar -> {
            context.assertTrue(ar.succeeded());
            createAsync.complete();
        });
        createAsync.await();

        Set<String> expectedPvcs = expectedPvcs();

        // trying to remove a volume from the JBOD storage
        volumes.remove(0);

        Kafka changedKafka = new KafkaBuilder(this.kafka)
                .editSpec()
                    .editKafka()
                        .withStorage(new JbodStorageBuilder().withVolumes(volumes).build())
                    .endKafka()
                .endSpec()
                .build();

        Crds.kafkaOperation(mockClient).inNamespace(NAMESPACE).withName(NAME).patch(changedKafka);

        Async updateAsync = context.async();
        this.kao.reconcileAssembly(new Reconciliation("test-trigger", ResourceType.KAFKA, NAMESPACE, NAME), ar -> {
            context.assertTrue(ar.succeeded());
            Set<String> pvcsNames = getPvcs(NAMESPACE, NAME).stream()
                    .map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toSet());
            context.assertEquals(expectedPvcs, pvcsNames);
            updateAsync.complete();
        });
    }

    @Test
    public void testUpdateVolumeIdJbod(TestContext context) {

        this.init();

        // first reconcile for cluster creation
        Async createAsync = context.async();
        this.kao.reconcileAssembly(new Reconciliation("test-trigger", ResourceType.KAFKA, NAMESPACE, NAME), ar -> {
            context.assertTrue(ar.succeeded());
            createAsync.complete();
        });
        createAsync.await();

        Set<String> expectedPvcs = expectedPvcs();

        // trying to update id for a volume from in the JBOD storage
        volumes.get(0).setId(3);

        Kafka changedKafka = new KafkaBuilder(this.kafka)
                .editSpec()
                    .editKafka()
                        .withStorage(new JbodStorageBuilder().withVolumes(volumes).build())
                    .endKafka()
                .endSpec()
                .build();

        Crds.kafkaOperation(mockClient).inNamespace(NAMESPACE).withName(NAME).patch(changedKafka);

        Async updateAsync = context.async();
        this.kao.reconcileAssembly(new Reconciliation("test-trigger", ResourceType.KAFKA, NAMESPACE, NAME), ar -> {
            context.assertTrue(ar.succeeded());
            Set<String> pvcsNames = getPvcs(NAMESPACE, NAME).stream()
                    .map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toSet());
            context.assertEquals(expectedPvcs, pvcsNames);
            updateAsync.complete();
        });
    }

    private Set<String> expectedPvcs() {
        Set<String> expectedPvcs = new HashSet<>();
        for (int i = 0; i < this.kafka.getSpec().getKafka().getReplicas(); i++) {
            int podId = i;
            for (SingleVolumeStorage volume : this.volumes) {
                if (volume instanceof PersistentClaimStorage) {
                    expectedPvcs.add(AbstractModel.VOLUME_NAME + "-" + volume.getId() + "-"
                            + KafkaCluster.kafkaPodName(NAME, podId));
                }
            }
        }
        return expectedPvcs;
    }

    private List<PersistentVolumeClaim> getPvcs(String namespace, String name) {
        String kafkaSsName = KafkaCluster.kafkaClusterName(name);
        Labels pvcSelector = Labels.forCluster(name).withKind(Kafka.RESOURCE_KIND).withName(kafkaSsName);
        return mockClient.persistentVolumeClaims().inNamespace(namespace).withLabels(pvcSelector.toMap())
                .list().getItems();
    }
}
