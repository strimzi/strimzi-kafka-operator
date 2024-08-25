/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.strimzi.api.kafka.model.kafka.EphemeralStorageBuilder;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageOverrideBuilder;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.model.nodepools.NodePoolUtils;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity", "checkstyle:JavaNCSS"})
@ParallelSuite
public class KafkaClusterStorageTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();

    private final static String NAMESPACE = "test";
    private final static String CLUSTER = "foo";
    private final static Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withName(CLUSTER)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withNewKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                                    .withName("plain")
                                    .withPort(9092)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(false)
                                    .build(),
                            new GenericKafkaListenerBuilder()
                                    .withName("tls")
                                    .withPort(9093)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(true)
                                    .build())
                .endKafka()
            .endSpec()
            .build();
    private final static KafkaNodePool POOL_CONTROLLERS = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("controllers")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.CONTROLLER)
            .endSpec()
            .build();
    private final static KafkaNodePool POOL_MIXED = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("mixed")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(2)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.CONTROLLER, ProcessRoles.BROKER)
            .endSpec()
            .build();
    private final static KafkaNodePool POOL_BROKERS = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("brokers")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.BROKER)
            .endSpec()
            .build();

    private static final List<KafkaPool> POOLS = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true, SHARED_ENV_PROVIDER);
    private final static KafkaCluster KC = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, POOLS, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);

    //////////
    // Tests
    //////////

    @ParallelTest
    public void testPvcNames() {
        List<PersistentVolumeClaim> pvcs = KC.generatePersistentVolumeClaims();
        assertThat(pvcs.size(), is(8));
        assertThat(pvcs.stream().map(pvc -> pvc.getMetadata().getName()).toList(), is(List.of("data-0-foo-controllers-0", "data-0-foo-controllers-1", "data-0-foo-controllers-2", "data-0-foo-mixed-3", "data-0-foo-mixed-4", "data-0-foo-brokers-5", "data-0-foo-brokers-6", "data-0-foo-brokers-7")));

        KafkaNodePool brokersNoJbod = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withStorage(new PersistentClaimStorageBuilder().withDeleteClaim(false).withId(0).withSize("100Gi").build())
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, POOL_MIXED, brokersNoJbod), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);

        pvcs = kc.generatePersistentVolumeClaims();
        assertThat(pvcs.size(), is(8));
        assertThat(pvcs.stream().map(pvc -> pvc.getMetadata().getName()).toList(), is(List.of("data-0-foo-controllers-0", "data-0-foo-controllers-1", "data-0-foo-controllers-2", "data-0-foo-mixed-3", "data-0-foo-mixed-4", "data-foo-brokers-5", "data-foo-brokers-6", "data-foo-brokers-7")));

        KafkaNodePool brokers2Disks = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withStorage(new JbodStorageBuilder().withVolumes(
                            new PersistentClaimStorageBuilder().withDeleteClaim(false).withId(0).withSize("100Gi").build(),
                            new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(1).withSize("100Gi").build())
                            .build())
                .endSpec()
                .build();
        pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers2Disks), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true, SHARED_ENV_PROVIDER);
        kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);

        pvcs = kc.generatePersistentVolumeClaims();
        assertThat(pvcs.size(), is(11));
        assertThat(pvcs.stream().map(pvc -> pvc.getMetadata().getName()).toList(), is(List.of("data-0-foo-controllers-0", "data-0-foo-controllers-1", "data-0-foo-controllers-2", "data-0-foo-mixed-3", "data-0-foo-mixed-4", "data-0-foo-brokers-5", "data-0-foo-brokers-6", "data-0-foo-brokers-7", "data-1-foo-brokers-5", "data-1-foo-brokers-6", "data-1-foo-brokers-7")));
    }

    @ParallelTest
    public void testGeneratePersistentVolumeClaimsPersistentWithClaimDeletion() {
        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewPersistentClaimStorage()
                        .withStorageClass("gp2-ssd")
                        .withDeleteClaim(true)
                        .withSize("100Gi")
                    .endPersistentClaimStorage()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = kc.generatePersistentVolumeClaims();
        assertThat(pvcs.size(), is(8));

        for (PersistentVolumeClaim pvc : pvcs) {
            if (pvc.getMetadata().getName().contains("brokers")) {
                assertThat(pvc.getSpec().getResources().getRequests().get("storage"), is(new Quantity("100Gi")));
                assertThat(pvc.getSpec().getStorageClassName(), is("gp2-ssd"));
                assertThat(pvc.getMetadata().getName().startsWith(VolumeUtils.DATA_VOLUME_NAME), is(true));
                assertThat(pvc.getMetadata().getOwnerReferences().size(), is(1));
                assertThat(pvc.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_DELETE_CLAIM), is("true"));
            }
        }
    }

    @ParallelTest
    public void testGeneratePersistentVolumeClaimsPersistentWithoutClaimDeletion() {
        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewPersistentClaimStorage()
                        .withStorageClass("gp2-ssd")
                        .withDeleteClaim(false)
                        .withSize("100Gi")
                    .endPersistentClaimStorage()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = kc.generatePersistentVolumeClaims();
        assertThat(pvcs.size(), is(8));

        for (PersistentVolumeClaim pvc : pvcs) {
            if (pvc.getMetadata().getName().contains("brokers")) {
                assertThat(pvc.getSpec().getResources().getRequests().get("storage"), is(new Quantity("100Gi")));
                assertThat(pvc.getSpec().getStorageClassName(), is("gp2-ssd"));
                assertThat(pvc.getMetadata().getName().startsWith(VolumeUtils.DATA_VOLUME_NAME), is(true));
                assertThat(pvc.getMetadata().getOwnerReferences().size(), is(0));
                assertThat(pvc.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_DELETE_CLAIM), is("false"));
            }
        }
    }

    @ParallelTest
    public void testGeneratePersistentVolumeClaimsPersistentWithOverride() {
        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewPersistentClaimStorage()
                        .withStorageClass("gp2-ssd")
                        .withDeleteClaim(false)
                        .withSize("100Gi")
                        .withOverrides(new PersistentClaimStorageOverrideBuilder()
                                .withBroker(6)
                                .withStorageClass("gp2-ssd-az1")
                                .build())
                    .endPersistentClaimStorage()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = kc.generatePersistentVolumeClaims();
        assertThat(pvcs.size(), is(8));

        for (PersistentVolumeClaim pvc : pvcs) {
            if (pvc.getMetadata().getName().contains("brokers")) {
                assertThat(pvc.getSpec().getResources().getRequests().get("storage"), is(new Quantity("100Gi")));
                assertThat(pvc.getMetadata().getName().startsWith(VolumeUtils.DATA_VOLUME_NAME), is(true));
                assertThat(pvc.getMetadata().getOwnerReferences().size(), is(0));
                assertThat(pvc.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_DELETE_CLAIM), is("false"));

                if (pvc.getMetadata().getName().contains("brokers-6"))  {
                    assertThat(pvc.getSpec().getStorageClassName(), is("gp2-ssd-az1"));
                } else {
                    assertThat(pvc.getSpec().getStorageClassName(), is("gp2-ssd"));
                }
            }
        }
    }

    @ParallelTest
    public void testGeneratePersistentVolumeClaimsJbod() {
        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewJbodStorage()
                        .withVolumes(new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd")
                                        .withDeleteClaim(false)
                                        .withId(0)
                                        .withSize("100Gi")
                                        .build(),
                                new PersistentClaimStorageBuilder()
                                        .withStorageClass("gp2-st1")
                                        .withDeleteClaim(true)
                                        .withId(1)
                                        .withSize("1000Gi")
                                        .build())
                    .endJbodStorage()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = kc.generatePersistentVolumeClaims();
        assertThat(pvcs.size(), is(11));

        for (PersistentVolumeClaim pvc : pvcs) {
            if (pvc.getMetadata().getName().contains("brokers")) {
                if (pvc.getMetadata().getName().contains("data-0")) {
                    assertThat(pvc.getSpec().getResources().getRequests().get("storage"), is(new Quantity("100Gi")));
                    assertThat(pvc.getMetadata().getName().startsWith(VolumeUtils.DATA_VOLUME_NAME), is(true));
                    assertThat(pvc.getMetadata().getOwnerReferences().size(), is(0));
                    assertThat(pvc.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_DELETE_CLAIM), is("false"));
                    assertThat(pvc.getSpec().getStorageClassName(), is("gp2-ssd"));
                } else if (pvc.getMetadata().getName().contains("data-1")) {
                    assertThat(pvc.getSpec().getResources().getRequests().get("storage"), is(new Quantity("1000Gi")));
                    assertThat(pvc.getMetadata().getName().startsWith(VolumeUtils.DATA_VOLUME_NAME), is(true));
                    assertThat(pvc.getMetadata().getOwnerReferences().size(), is(1));
                    assertThat(pvc.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_DELETE_CLAIM), is("true"));
                    assertThat(pvc.getSpec().getStorageClassName(), is("gp2-st1"));
                }
            }
        }
    }

    @ParallelTest
    public void testGeneratePersistentVolumeClaimsJbodWithOverrides() {
        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewJbodStorage()
                        .withVolumes(new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd")
                                        .withDeleteClaim(false)
                                        .withId(0)
                                        .withSize("100Gi")
                                        .withOverrides(new PersistentClaimStorageOverrideBuilder().withBroker(6).withStorageClass("gp2-ssd-az1").build())
                                        .build(),
                                new PersistentClaimStorageBuilder()
                                        .withStorageClass("gp2-st1")
                                        .withDeleteClaim(true)
                                        .withId(1)
                                        .withSize("1000Gi")
                                        .withOverrides(new PersistentClaimStorageOverrideBuilder().withBroker(6).withStorageClass("gp2-st1-az1").build())
                                        .build())
                    .endJbodStorage()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = kc.generatePersistentVolumeClaims();
        assertThat(pvcs.size(), is(11));

        for (PersistentVolumeClaim pvc : pvcs) {
            if (pvc.getMetadata().getName().contains("brokers")) {
                if (pvc.getMetadata().getName().contains("data-0")) {
                    assertThat(pvc.getSpec().getResources().getRequests().get("storage"), is(new Quantity("100Gi")));
                    assertThat(pvc.getMetadata().getName().startsWith(VolumeUtils.DATA_VOLUME_NAME), is(true));
                    assertThat(pvc.getMetadata().getOwnerReferences().size(), is(0));
                    assertThat(pvc.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_DELETE_CLAIM), is("false"));

                    if (pvc.getMetadata().getName().contains("brokers-6")) {
                        assertThat(pvc.getSpec().getStorageClassName(), is("gp2-ssd-az1"));
                    } else {
                        assertThat(pvc.getSpec().getStorageClassName(), is("gp2-ssd"));
                    }
                } else if (pvc.getMetadata().getName().contains("data-1")) {
                    assertThat(pvc.getSpec().getResources().getRequests().get("storage"), is(new Quantity("1000Gi")));
                    assertThat(pvc.getMetadata().getName().startsWith(VolumeUtils.DATA_VOLUME_NAME), is(true));
                    assertThat(pvc.getMetadata().getOwnerReferences().size(), is(1));
                    assertThat(pvc.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_DELETE_CLAIM), is("true"));

                    if (pvc.getMetadata().getName().contains("brokers-6")) {
                        assertThat(pvc.getSpec().getStorageClassName(), is("gp2-st1-az1"));
                    } else {
                        assertThat(pvc.getSpec().getStorageClassName(), is("gp2-st1"));
                    }
                }
            }
        }
    }

    @ParallelTest
    public void testPvcsWithEmptyStorageSelector() {
        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewPersistentClaimStorage()
                        .withSelector(Map.of())
                        .withSize("100Gi")
                    .endPersistentClaimStorage()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = kc.generatePersistentVolumeClaims();
        assertThat(pvcs.size(), is(8));

        for (PersistentVolumeClaim pvc : pvcs) {
            if (pvc.getMetadata().getName().contains("brokers")) {
                assertThat(pvc.getSpec().getSelector(), is(nullValue()));
            }
        }
    }

    @ParallelTest
    public void testPvcsWithSetStorageSelector() {
        Map<String, String> selector = Map.of("foo", "bar");
        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewPersistentClaimStorage()
                        .withSelector(selector)
                        .withSize("100Gi")
                    .endPersistentClaimStorage()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = kc.generatePersistentVolumeClaims();
        assertThat(pvcs.size(), is(8));

        for (PersistentVolumeClaim pvc : pvcs) {
            if (pvc.getMetadata().getName().contains("brokers")) {
                assertThat(pvc.getSpec().getSelector().getMatchLabels(), is(selector));
            } else {
                assertThat(pvc.getSpec().getSelector(), is(nullValue()));
            }
        }
    }

    @ParallelTest
    public void testGeneratePersistentVolumeClaimsJbodWithTemplate() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewTemplate()
                            .withNewPersistentVolumeClaim()
                                .withNewMetadata()
                                    .withLabels(Map.of("testLabel", "testValue"))
                                    .withAnnotations(Map.of("testAnno", "testValue"))
                                .endMetadata()
                            .endPersistentVolumeClaim()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();
        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewJbodStorage()
                        .withVolumes(new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd")
                                        .withDeleteClaim(false)
                                        .withId(0)
                                        .withSize("100Gi")
                                        .build(),
                                new PersistentClaimStorageBuilder()
                                        .withDeleteClaim(true)
                                        .withId(1)
                                        .withSize("1000Gi")
                                        .build())
                    .endJbodStorage()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = kc.generatePersistentVolumeClaims();
        assertThat(pvcs.size(), is(11));

        for (PersistentVolumeClaim pvc : pvcs) {
            assertThat(pvc.getMetadata().getLabels().get("testLabel"), is("testValue"));
            assertThat(pvc.getMetadata().getAnnotations().get("testAnno"), is("testValue"));
        }
    }

    @ParallelTest
    public void testGeneratePersistentVolumeClaimsJbodWithTemplateInKafkaAndNodePool() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewTemplate()
                            .withNewPersistentVolumeClaim()
                                .withNewMetadata()
                                    .withLabels(Map.of("testLabel", "testValue"))
                                    .withAnnotations(Map.of("testAnno", "testValue"))
                                .endMetadata()
                            .endPersistentVolumeClaim()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();
        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewJbodStorage()
                        .withVolumes(new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd")
                                        .withDeleteClaim(false)
                                        .withId(0)
                                        .withSize("100Gi")
                                        .build(),
                                new PersistentClaimStorageBuilder()
                                        .withDeleteClaim(true)
                                        .withId(1)
                                        .withSize("1000Gi")
                                        .build())
                    .endJbodStorage()
                    .withNewTemplate()
                        .withNewPersistentVolumeClaim()
                            .withNewMetadata()
                                .withLabels(Map.of("testLabel", "testValue-pool"))
                                .withAnnotations(Map.of("testAnno", "testValue-pool"))
                            .endMetadata()
                        .endPersistentVolumeClaim()
                    .endTemplate()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = kc.generatePersistentVolumeClaims();
        assertThat(pvcs.size(), is(11));

        for (PersistentVolumeClaim pvc : pvcs) {
            if (pvc.getMetadata().getName().contains("brokers")) {
                assertThat(pvc.getMetadata().getLabels().get("testLabel"), is("testValue-pool"));
                assertThat(pvc.getMetadata().getAnnotations().get("testAnno"), is("testValue-pool"));
            } else {
                assertThat(pvc.getMetadata().getLabels().get("testLabel"), is("testValue"));
                assertThat(pvc.getMetadata().getAnnotations().get("testAnno"), is("testValue"));
            }
        }
    }

    @ParallelTest
    public void testGeneratePersistentVolumeClaimsJbodWithTemplateInNodePoolOnly() {
        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewJbodStorage()
                        .withVolumes(new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd")
                                        .withDeleteClaim(false)
                                        .withId(0)
                                        .withSize("100Gi")
                                        .build(),
                                new PersistentClaimStorageBuilder()
                                        .withDeleteClaim(true)
                                        .withId(1)
                                        .withSize("1000Gi")
                                        .build())
                    .endJbodStorage()
                    .withNewTemplate()
                        .withNewPersistentVolumeClaim()
                            .withNewMetadata()
                                .withLabels(Map.of("testLabel", "testValue-pool"))
                                .withAnnotations(Map.of("testAnno", "testValue-pool"))
                            .endMetadata()
                        .endPersistentVolumeClaim()
                    .endTemplate()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = kc.generatePersistentVolumeClaims();
        assertThat(pvcs.size(), is(11));

        for (PersistentVolumeClaim pvc : pvcs) {
            if (pvc.getMetadata().getName().contains("brokers")) {
                assertThat(pvc.getMetadata().getLabels().get("testLabel"), is("testValue-pool"));
                assertThat(pvc.getMetadata().getAnnotations().get("testAnno"), is("testValue-pool"));
            } else {
                assertThat(pvc.getMetadata().getLabels().get("testLabel"), is(nullValue()));
                assertThat(pvc.getMetadata().getAnnotations().get("testAnno"), is(nullValue()));
            }
        }
    }

    @ParallelTest
    public void testGeneratePersistentVolumeClaimsJbodWithoutVolumes() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withStorage(new JbodStorageBuilder().withVolumes(List.of()).build())
                .endSpec()
                .build();

            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);
        });
    }

    @ParallelTest
    public void testEphemeralStorage()    {
        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewEphemeralStorage().endEphemeralStorage()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);

        // Test generated SPS
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, brokerId -> new HashMap<>());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);
            for (Pod pod : pods) {
                if (pod.getMetadata().getName().contains("brokers"))    {
                    assertThat(pod.getSpec().getVolumes().stream().filter(v -> "data".equals(v.getName())).findFirst().orElseThrow().getEmptyDir(), is(notNullValue()));
                    assertThat(pod.getSpec().getVolumes().get(0).getEmptyDir().getSizeLimit(), is(nullValue()));
                }
            }
        }

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = kc.generatePersistentVolumeClaims();
        assertThat(pvcs.size(), is(5));
        assertThat(pvcs.stream().filter(pvc -> pvc.getMetadata().getName().contains("brokers")).count(), is(0L));
    }

    @ParallelTest
    public void testGeneratePodSetWithSetSizeLimit() {
        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewEphemeralStorage()
                        .withSizeLimit("1Gi")
                    .endEphemeralStorage()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);

        // Test generated SPS
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, brokerId -> new HashMap<>());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);
            for (Pod pod : pods) {
                if (pod.getMetadata().getName().contains("brokers"))    {
                    assertThat(pod.getSpec().getVolumes().stream().filter(v -> "data".equals(v.getName())).findFirst().orElseThrow().getEmptyDir(), is(notNullValue()));
                    assertThat(pod.getSpec().getVolumes().get(0).getEmptyDir().getSizeLimit(), is(new Quantity("1", "Gi")));
                }
            }
        }
    }

    @ParallelTest
    public void testStorageValidationAfterInitialDeployment() {
        assertThrows(InvalidResourceException.class, () -> {
            Storage oldStorage = new JbodStorageBuilder()
                    .withVolumes(new PersistentClaimStorageBuilder().withSize("100Gi").build())
                    .build();

            KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withStorage(new JbodStorageBuilder().withVolumes(List.of()).build())
                .endSpec()
                .build();

            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of("foo-brokers", oldStorage),
                Map.of("foo-brokers", IntStream.range(5, 7).mapToObj(i -> "foo-brokers-" + i).toList()), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);
        });
    }

    @ParallelTest
    public void testStorageReverting() {
        Storage jbod = new JbodStorageBuilder().withVolumes(
                new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("100Gi").build(),
                new PersistentClaimStorageBuilder().withStorageClass("gp2-st1").withDeleteClaim(true).withId(1).withSize("1000Gi").build())
                .build();
        Storage ephemeral = new EphemeralStorageBuilder().build();
        Storage persistent = new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("100Gi").build();

        // Test Storage changes and how they are reverted
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        // Added to suppress other warning conditions
                        .withConfig(Map.of("default.replication.factor", 3, "min.insync.replicas", 2))
                    .endKafka()
                .endSpec()
                .build();

        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withStorage(jbod)
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(CLUSTER + "-brokers", ephemeral),
            Map.of(CLUSTER + "-brokers", IntStream.range(5, 7).mapToObj(i -> CLUSTER + "-brokers-" + i).toList()), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);

        // Storage is reverted
        assertThat(kc.getStorageByPoolName().size(), is(3));
        assertThat(kc.getStorageByPoolName().get("controllers"), is(notNullValue()));
        assertThat(kc.getStorageByPoolName().get("mixed"), is(notNullValue()));
        assertThat(kc.getStorageByPoolName().get("brokers"), is(ephemeral));

        // Warning status condition is set
        System.out.println(kc.getWarningConditions());
        assertThat(kc.getWarningConditions().size(), is(1));
        assertThat(kc.getWarningConditions().get(0).getReason(), is("KafkaStorage"));

        brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withStorage(jbod)
                .endSpec()
                .build();
        pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(CLUSTER + "-brokers", persistent),
            Map.of(CLUSTER + "-brokers", IntStream.range(5, 7).mapToObj(i -> CLUSTER + "-brokers-" + i).toList()), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true, SHARED_ENV_PROVIDER);
        kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);

        // Storage is reverted
        assertThat(kc.getStorageByPoolName().get("brokers"), is(persistent));

        // Warning status condition is set
        assertThat(kc.getWarningConditions().size(), is(1));
        assertThat(kc.getWarningConditions().get(0).getReason(), is("KafkaStorage"));

        brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withStorage(ephemeral)
                .endSpec()
                .build();
        pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(CLUSTER + "-brokers", jbod),
            Map.of(CLUSTER + "-brokers", IntStream.range(5, 7).mapToObj(i -> CLUSTER + "-brokers-" + i).toList()), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true, SHARED_ENV_PROVIDER);
        kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);

        // Storage is reverted
        assertThat(kc.getStorageByPoolName().get("brokers"), is(jbod));

        // Warning status condition is set
        assertThat(kc.getWarningConditions().size(), is(1));
        assertThat(kc.getWarningConditions().get(0).getReason(), is("KafkaStorage"));

        brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withStorage(persistent)
                .endSpec()
                .build();
        pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(CLUSTER + "-brokers", jbod),
            Map.of(CLUSTER + "-brokers", IntStream.range(5, 7).mapToObj(i -> CLUSTER + "-brokers-" + i).toList()), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true, SHARED_ENV_PROVIDER);
        kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);

        // Storage is reverted
        assertThat(kc.getStorageByPoolName().get("brokers"), is(jbod));

        // Warning status condition is set
        assertThat(kc.getWarningConditions().size(), is(1));
        assertThat(kc.getWarningConditions().get(0).getReason(), is("KafkaStorage"));
    }
}
