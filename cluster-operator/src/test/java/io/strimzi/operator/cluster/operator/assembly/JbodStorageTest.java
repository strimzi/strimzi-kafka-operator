/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.model.storage.JbodStorageBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.test.mockkube.MockKube;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(VertxExtension.class)
public class JbodStorageTest {

    private static final String NAMESPACE = "test-jbod-storage";
    private static final String NAME = "my-kafka";
    private static final KafkaVersion.Lookup VERSIONS = new KafkaVersion.Lookup(
            new StringReader(KafkaVersionTestUtils.getKafkaVersionYaml()),
            KafkaVersionTestUtils.getKafkaImageMap(),
            emptyMap(),
            emptyMap(),
            emptyMap()) { };

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
                .withCustomResourceDefinition(kafkaAssemblyCrd, Kafka.class, KafkaList.class, DoneableKafka.class)
                .end()
                .build();

        Crds.kafkaOperation(this.mockClient).inNamespace(NAMESPACE).withName(NAME).create(this.kafka);

        this.vertx = Vertx.vertx();

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(false, KubernetesVersion.V1_9);
        // creating the Kafka operator
        ResourceOperatorSupplier ros =
                new ResourceOperatorSupplier(this.vertx, this.mockClient,
                        ResourceUtils.zookeeperLeaderFinder(this.vertx, this.mockClient),
                        ResourceUtils.adminClientProvider(),
                        pfa, 60_000L);

        this.kao = new KafkaAssemblyOperator(this.vertx, pfa, new MockCertManager(), new PasswordGenerator(10, "a", "a"), ros, ResourceUtils.dummyClusterOperatorConfig(VERSIONS, 2_000));
    }

    @Test
    public void testCreatePersistentVolumeClaims(VertxTestContext context) {

        this.init();

        Checkpoint async = context.checkpoint();
        this.kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME)).setHandler(ar -> {
            context.verify(() -> assertThat(ar.succeeded(), is(true)));

            List<PersistentVolumeClaim> pvcs = getPvcs(NAMESPACE, NAME);

            for (int i = 0; i < this.kafka.getSpec().getKafka().getReplicas(); i++) {
                int podId = i;
                for (SingleVolumeStorage volume : this.volumes) {
                    if (volume instanceof PersistentClaimStorage) {
                        context.verify(() -> assertThat(pvcs.stream().anyMatch(pvc -> {
                            String pvcName = ModelUtils.getVolumePrefix(volume.getId()) + "-"
                                    + KafkaCluster.kafkaPodName(NAME, podId);
                            boolean isDeleteClaim = ((PersistentClaimStorage) volume).isDeleteClaim();

                            boolean namesMatch = pvc.getMetadata().getName().equals(pvcName);
                            return namesMatch && Annotations.booleanAnnotation(pvc, AbstractModel.ANNO_STRIMZI_IO_DELETE_CLAIM,
                                    false, AbstractModel.ANNO_CO_STRIMZI_IO_DELETE_CLAIM) == isDeleteClaim;
                        }), is(true)));
                    }
                }
            }
            async.flag();
        });
    }

    @Test
    public void testAddVolumeToJbod(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {

        this.init();

        // first reconcile for cluster creation
        CountDownLatch createAsync = new CountDownLatch(1);
        this.kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME)).setHandler(ar -> {
            context.verify(() -> assertThat(ar.succeeded(), is(true)));
            createAsync.countDown();
        });
        if (!createAsync.await(60, TimeUnit.SECONDS)) {
            context.failNow(new Throwable("Test timeout"));
        }

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

        Set<String> expectedPvcs = expectedPvcs(changedKafka);

        Crds.kafkaOperation(mockClient).inNamespace(NAMESPACE).withName(NAME).patch(changedKafka);

        Checkpoint updateAsync = context.checkpoint();
        this.kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME)).setHandler(ar -> {
            context.verify(() -> assertThat(ar.succeeded(), is(true)));
            List<PersistentVolumeClaim> pvcs = getPvcs(NAMESPACE, NAME);
            Set<String> pvcsNames = pvcs.stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toSet());
            context.verify(() -> assertThat(pvcsNames, is(expectedPvcs)));
            updateAsync.flag();
        });
    }

    @Test
    public void testRemoveVolumeFromJbod(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {

        this.init();

        // first reconcile for cluster creation
        CountDownLatch createAsync = new CountDownLatch(1);
        this.kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME)).setHandler(ar -> {
            context.verify(() -> assertThat(ar.succeeded(), is(true)));
            createAsync.countDown();
        });
        if (!createAsync.await(60, TimeUnit.SECONDS)) {
            context.failNow(new Throwable("Test timeout"));
        }

        // trying to remove a volume from the JBOD storage
        volumes.remove(0);

        Kafka changedKafka = new KafkaBuilder(this.kafka)
                .editSpec()
                    .editKafka()
                        .withStorage(new JbodStorageBuilder().withVolumes(volumes).build())
                    .endKafka()
                .endSpec()
                .build();

        Set<String> expectedPvcs = expectedPvcs(changedKafka);

        Crds.kafkaOperation(mockClient).inNamespace(NAMESPACE).withName(NAME).patch(changedKafka);

        Checkpoint updateAsync = context.checkpoint();
        this.kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME)).setHandler(ar -> {
            context.verify(() -> assertThat(ar.succeeded(), is(true)));
            Set<String> pvcsNames = getPvcs(NAMESPACE, NAME).stream()
                    .map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toSet());
            context.verify(() -> assertThat(pvcsNames, is(expectedPvcs)));
            updateAsync.flag();
        });
    }

    @Test
    public void testUpdateVolumeIdJbod(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {

        this.init();

        // first reconcile for cluster creation
        CountDownLatch createAsync = new CountDownLatch(1);
        this.kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME)).setHandler(ar -> {
            context.verify(() -> assertThat(ar.succeeded(), is(true)));
            createAsync.countDown();
        });
        if (!createAsync.await(60, TimeUnit.SECONDS)) {
            context.failNow(new Throwable("Test timeout"));
        }

        // trying to update id for a volume from in the JBOD storage
        volumes.get(0).setId(3);

        Kafka changedKafka = new KafkaBuilder(this.kafka)
                .editSpec()
                    .editKafka()
                        .withStorage(new JbodStorageBuilder().withVolumes(volumes).build())
                    .endKafka()
                .endSpec()
                .build();

        Set<String> expectedPvcs = expectedPvcs(changedKafka);

        Crds.kafkaOperation(mockClient).inNamespace(NAMESPACE).withName(NAME).patch(changedKafka);

        Checkpoint updateAsync = context.checkpoint();
        this.kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME)).setHandler(ar -> {
            context.verify(() -> assertThat(ar.succeeded(), is(true)));
            Set<String> pvcsNames = getPvcs(NAMESPACE, NAME).stream()
                    .map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toSet());
            context.verify(() -> assertThat(pvcsNames, is(expectedPvcs)));
            updateAsync.flag();
        });
    }

    private Set<String> expectedPvcs(Kafka kafka) {
        Set<String> expectedPvcs = new HashSet<>();
        for (int i = 0; i < kafka.getSpec().getKafka().getReplicas(); i++) {
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
