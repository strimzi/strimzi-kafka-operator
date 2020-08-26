/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetSpec;
import io.fabric8.kubernetes.api.model.apps.StatefulSetStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.storage.EphemeralStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperator;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.StatefulSetOperator;
import io.strimzi.operator.cluster.operator.resource.ZookeeperLeaderFinder;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.test.mockkube.MockKube;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.strimzi.api.kafka.model.storage.Storage.deleteClaim;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@ExtendWith(VertxExtension.class)
@SuppressWarnings("checkstyle:ClassFanOutComplexity")
public class KafkaAssemblyOperatorMockTest {

    private static final Logger LOGGER = LogManager.getLogger(KafkaAssemblyOperatorMockTest.class);

    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-cluster";

    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static Vertx vertx;

    private final KubernetesVersion kubernetesVersion = KubernetesVersion.V1_9;

    private int zkReplicas;
    private SingleVolumeStorage zkStorage;

    private int kafkaReplicas;
    private Storage kafkaStorage;

    private ResourceRequirements resources;
    private KubernetesClient client;

    private KafkaAssemblyOperator operator;

    /*
     * Params contains the parameterized fields of different configurations of a Kafka and Zookeeper Strimzi cluster
     */
    public static class Params {
        private final int zkReplicas;
        private final SingleVolumeStorage zkStorage;

        private final int kafkaReplicas;
        private final Storage kafkaStorage;
        private ResourceRequirements resources;

        public Params(int zkReplicas,
                      SingleVolumeStorage zkStorage,
                      int kafkaReplicas,
                      Storage kafkaStorage,
                      ResourceRequirements resources) {
            this.zkReplicas = zkReplicas;
            this.zkStorage = zkStorage;
            this.kafkaReplicas = kafkaReplicas;
            this.kafkaStorage = kafkaStorage;
            this.resources = resources;
        }

        public String toString() {
            return "zkReplicas=" + zkReplicas +
                    ",zkStorage=" + zkStorage +
                    ",kafkaReplicas=" + kafkaReplicas +
                    ",kafkaStorage=" + kafkaStorage +
                    ",resources=" + resources;
        }
    }

    public static Iterable<KafkaAssemblyOperatorMockTest.Params> data() {
        int[] replicas = {1, 3};
        Storage[] kafkaStorageConfigs = {
            new EphemeralStorage(),
            new PersistentClaimStorageBuilder()
                .withSize("123")
                .withStorageClass("foo")
                .withDeleteClaim(true)
                .build(),
            new PersistentClaimStorageBuilder()
                .withSize("123")
                .withStorageClass("foo")
                .withDeleteClaim(false)
                .build()
        };
        SingleVolumeStorage[] zkStorageConfigs = {
            new EphemeralStorage(),
            new PersistentClaimStorageBuilder()
                    .withSize("123")
                    .withStorageClass("foo")
                    .withDeleteClaim(true)
                    .build(),
            new PersistentClaimStorageBuilder()
                    .withSize("123")
                    .withStorageClass("foo")
                    .withDeleteClaim(false)
                    .build()
        };
        ResourceRequirements[] resources = {
            new ResourceRequirementsBuilder()
                .addToLimits("cpu", new Quantity("5000m"))
                .addToLimits("memory", new Quantity("5000m"))
                .addToRequests("cpu", new Quantity("5000"))
                .addToRequests("memory", new Quantity("5000m"))
                .build()
        };
        List<KafkaAssemblyOperatorMockTest.Params> result = new ArrayList();

        for (int zkReplica : replicas) {
            for (SingleVolumeStorage zkStorage : zkStorageConfigs) {
                for (int kafkaReplica : replicas) {
                    for (Storage kafkaStorage : kafkaStorageConfigs) {
                        for (ResourceRequirements resource : resources) {
                            result.add(new KafkaAssemblyOperatorMockTest.Params(
                                    zkReplica, zkStorage,
                                    kafkaReplica, kafkaStorage, resource));
                        }
                    }
                }
            }
        }

        return result;
    }

    public void setFields(KafkaAssemblyOperatorMockTest.Params params) {
        this.zkReplicas = params.zkReplicas;
        this.zkStorage = params.zkStorage;

        this.kafkaReplicas = params.kafkaReplicas;
        this.kafkaStorage = params.kafkaStorage;

        this.resources = params.resources;
    }

    private Kafka cluster;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }

    /*
     * init is equivalent to a @BeforeEach method
     * since this is a parameterized set, the tests params are only available at test start
     * This must be called before each test
     */
    private void init(Params params) {
        setFields(params);

        cluster = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(NAMESPACE)
                    .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(kafkaReplicas)
                        .withStorage(kafkaStorage)
                        .withMetrics(singletonMap("foo", "bar"))
                        .withResources(resources)
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(zkReplicas)
                        .withStorage(zkStorage)
                        .withMetrics(singletonMap("foo", "bar"))
                    .endZookeeper()
                    .withNewEntityOperator()
                        .withNewTopicOperator()
                        .endTopicOperator()
                        .withNewUserOperator()
                        .endUserOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        CustomResourceDefinition kafkaAssemblyCrd = Crds.kafka();

        client = new MockKube()
                .withCustomResourceDefinition(kafkaAssemblyCrd, Kafka.class, KafkaList.class, DoneableKafka.class)
                    .withInitialInstances(Collections.singleton(cluster))
                .end()
                .build();

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, kubernetesVersion);
        ResourceOperatorSupplier supplier = supplierWithMocks();
        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);
        operator = new KafkaAssemblyOperator(vertx, pfa, new MockCertManager(),
                new PasswordGenerator(10, "a", "a"), supplier, config);
    }

    private ResourceOperatorSupplier supplierWithMocks() {
        ZookeeperLeaderFinder leaderFinder = ResourceUtils.zookeeperLeaderFinder(vertx, client);
        return new ResourceOperatorSupplier(vertx, client, leaderFinder,
                ResourceUtils.adminClientProvider(), ResourceUtils.zookeeperScalerProvider(),
                ResourceUtils.metricsProvider(), new PlatformFeaturesAvailability(true, kubernetesVersion),
                2_000);
    }

    private void assertResourceRequirements(VertxTestContext context, String statefulSetName) {
        context.verify(() -> {
            StatefulSet statefulSet = client.apps().statefulSets().inNamespace(NAMESPACE).withName(statefulSetName).get();
            assertThat(statefulSet, is(notNullValue()));
            ResourceRequirements requirements = statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).getResources();
            if (resources != null && resources.getRequests() != null) {
                assertThat(requirements.getRequests(), hasEntry("cpu", resources.getRequests().get("cpu")));
                assertThat(requirements.getRequests(), hasEntry("memory", resources.getRequests().get("memory")));
            }
            if (resources != null && resources.getLimits() != null) {
                assertThat(requirements.getLimits(), hasEntry("cpu", resources.getLimits().get("cpu")));
                assertThat(requirements.getLimits(), hasEntry("memory", resources.getLimits().get("memory")));
            }
        });
    }

    private Future<Void> initialReconcile(VertxTestContext context) {
        LOGGER.info("Reconciling initially -> create");
        return operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                StatefulSet kafkaSts = client.apps().statefulSets().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaClusterName(CLUSTER_NAME)).get();
                kafkaSts.setStatus(new StatefulSetStatus());

                assertThat(kafkaSts, is(notNullValue()));
                assertThat(kafkaSts.getSpec().getTemplate().getMetadata().getAnnotations(), hasEntry(StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, "0"));
                assertThat(kafkaSts.getSpec().getTemplate().getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));
                assertThat(kafkaSts.getSpec().getTemplate().getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0"));

                StatefulSet zkSts = client.apps().statefulSets().inNamespace(NAMESPACE).withName(ZookeeperCluster.zookeeperClusterName(CLUSTER_NAME)).get();
                assertThat(zkSts, is(notNullValue()));
                assertThat(zkSts.getSpec().getTemplate().getMetadata().getAnnotations(), hasEntry(StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, "0"));
                assertThat(zkSts.getSpec().getTemplate().getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));

                assertThat(client.configMaps().inNamespace(NAMESPACE).withName(KafkaCluster.metricAndLogConfigsName(CLUSTER_NAME)).get(), is(notNullValue()));
                assertThat(client.configMaps().inNamespace(NAMESPACE).withName(ZookeeperCluster.zookeeperMetricAndLogConfigsName(CLUSTER_NAME)).get(), is(notNullValue()));
                assertResourceRequirements(context, KafkaCluster.kafkaClusterName(CLUSTER_NAME));
                assertThat(client.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.clientsCaKeySecretName(CLUSTER_NAME)).get(), is(notNullValue()));
                assertThat(client.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.clientsCaCertSecretName(CLUSTER_NAME)).get(), is(notNullValue()));
                assertThat(client.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.clusterCaCertSecretName(CLUSTER_NAME)).get(), is(notNullValue()));
                assertThat(client.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.brokersSecretName(CLUSTER_NAME)).get(), is(notNullValue()));
                assertThat(client.secrets().inNamespace(NAMESPACE).withName(ZookeeperCluster.nodesSecretName(CLUSTER_NAME)).get(), is(notNullValue()));
            })));
    }

    /** Create a cluster from a Kafka */
    @ParameterizedTest
    @MethodSource("data")
    public void testReconcile(Params params, VertxTestContext context) {
        init(params);

        Checkpoint async = context.checkpoint();
        initialReconcile(context)
            .onComplete(context.succeeding())
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> async.flag()));
    }
    
    @ParameterizedTest
    @MethodSource("data")
    public void testReconcileReplacesAllDeletedSecrets(Params params, VertxTestContext context) {
        init(params);

        initialReconcileThenDeleteSecretsThenReconcile(context,
                KafkaCluster.clientsCaKeySecretName(CLUSTER_NAME),
                KafkaCluster.clientsCaCertSecretName(CLUSTER_NAME),
                KafkaCluster.clusterCaCertSecretName(CLUSTER_NAME),
                KafkaCluster.brokersSecretName(CLUSTER_NAME),
                ZookeeperCluster.nodesSecretName(CLUSTER_NAME),
                ClusterOperator.secretName(CLUSTER_NAME));
    }

    /**
     * Test the operator re-creates secrets if they get deleted
     */
    private void initialReconcileThenDeleteSecretsThenReconcile(VertxTestContext context, String... secrets) {
        Checkpoint async = context.checkpoint();

        initialReconcile(context)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                for (String secret: secrets) {
                    client.secrets().inNamespace(NAMESPACE).withName(secret).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
                    assertThat("Expected secret " + secret + " to not exist",
                            client.secrets().inNamespace(NAMESPACE).withName(secret).get(), is(nullValue()));
                }
                LOGGER.info("Reconciling again -> update");
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                for (String secret: secrets) {
                    assertThat("Expected secret " + secret + " to have been recreated",
                            client.secrets().inNamespace(NAMESPACE).withName(secret).get(), is(notNullValue()));
                }
                async.flag();
            })));
    }

    /**
     * Test the operator re-creates services if they get deleted
     */
    private void initialReconcileThenDeleteServicesThenReconcile(VertxTestContext context, String... services) {
        Checkpoint async = context.checkpoint();

        initialReconcile(context)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                for (String service : services) {
                    client.services().inNamespace(NAMESPACE).withName(service).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
                    assertThat("Expected service " + service + " to be not exist",
                            client.services().inNamespace(NAMESPACE).withName(service).get(), is(nullValue()));
                }
                LOGGER.info("Reconciling again -> update");
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                for (String service: services) {
                    assertThat("Expected service " + service + " to have been recreated",
                            client.services().inNamespace(NAMESPACE).withName(service).get(), is(notNullValue()));
                }
                async.flag();
            })));
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testReconcileReplacesDeletedZookeeperServices(Params params, VertxTestContext context) {
        init(params);

        initialReconcileThenDeleteServicesThenReconcile(context,
                ZookeeperCluster.serviceName(CLUSTER_NAME),
                ZookeeperCluster.headlessServiceName(CLUSTER_NAME));
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testReconcileReplacesDeletedKafkaServices(Params params, VertxTestContext context) {
        init(params);

        initialReconcileThenDeleteServicesThenReconcile(context,
                KafkaCluster.serviceName(CLUSTER_NAME),
                KafkaCluster.headlessServiceName(CLUSTER_NAME));
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testReconcileReplacesDeletedZookeeperStatefulSet(Params params, VertxTestContext context) {
        init(params);

        String statefulSet = ZookeeperCluster.zookeeperClusterName(CLUSTER_NAME);
        initialReconcileThenDeleteStatefulSetsThenReconcile(context, statefulSet);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testReconcileReplacesDeletedKafkaStatefulSet(Params params, VertxTestContext context) {
        init(params);

        String statefulSet = KafkaCluster.kafkaClusterName(CLUSTER_NAME);
        initialReconcileThenDeleteStatefulSetsThenReconcile(context, statefulSet);
    }

    private void initialReconcileThenDeleteStatefulSetsThenReconcile(VertxTestContext context, String statefulSet) {
        Checkpoint async = context.checkpoint();

        initialReconcile(context)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                client.apps().statefulSets().inNamespace(NAMESPACE).withName(statefulSet).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
                assertThat("Expected sts " + statefulSet + " should not exist",
                        client.apps().statefulSets().inNamespace(NAMESPACE).withName(statefulSet).get(), is(nullValue()));

                LOGGER.info("Reconciling again -> update");
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat("Expected sts " + statefulSet + " should have been re-created",
                        client.apps().statefulSets().inNamespace(NAMESPACE).withName(statefulSet).get(), is(notNullValue()));
                async.flag();
            })));
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testReconcileUpdatesKafkaPersistentVolumes(Params params, VertxTestContext context) {
        init(params);
        assumeTrue(kafkaStorage instanceof PersistentClaimStorage, "Parameterized Test only runs for Params with Kafka Persistent storage");

        String originalStorageClass = Storage.storageClass(kafkaStorage);

        Checkpoint async = context.checkpoint();
        initialReconcile(context)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertStorageClass(context, KafkaCluster.kafkaClusterName(CLUSTER_NAME), originalStorageClass);

                // Try to update the storage class
                String changedClass = originalStorageClass + "2";

                Kafka patchedPersistenceKafka = new KafkaBuilder(cluster)
                        .editSpec()
                            .editKafka()
                                .withNewPersistentClaimStorage()
                                    .withStorageClass(changedClass)
                                    .withSize("123")
                                .endPersistentClaimStorage()
                            .endKafka()
                        .endSpec()
                        .build();
                kafkaAssembly(NAMESPACE, CLUSTER_NAME).patch(patchedPersistenceKafka);

                LOGGER.info("Updating with changed storage class");
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Check the storage class was not changed
                assertStorageClass(context, KafkaCluster.kafkaClusterName(CLUSTER_NAME), originalStorageClass);
                async.flag();
            })));
    }

    private Resource<Kafka, DoneableKafka> kafkaAssembly(String namespace, String name) {
        CustomResourceDefinition crd = client.customResourceDefinitions().withName(Kafka.CRD_NAME).get();
        return client.customResources(CustomResourceDefinitionContext.fromCrd(crd), Kafka.class, KafkaList.class, DoneableKafka.class)
                .inNamespace(namespace).withName(name);
    }

    private void assertStorageClass(VertxTestContext context, String statefulSetName, String expectedClass) {
        StatefulSet statefulSet = client.apps().statefulSets().inNamespace(NAMESPACE).withName(statefulSetName).get();
        context.verify(() -> {
            assertThat(statefulSet, is(notNullValue()));
            // Check the storage class is initially "foo"
            List<PersistentVolumeClaim> volumeClaimTemplates = statefulSet.getSpec().getVolumeClaimTemplates();
            assertThat(volumeClaimTemplates, hasSize(1));
            assertThat(volumeClaimTemplates.get(0).getSpec().getStorageClassName(), is(expectedClass));
        });
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testReconcileUpdatesKafkaStorageType(Params params, VertxTestContext context) {
        init(params);

        AtomicReference<List<PersistentVolumeClaim>> originalPVCs = new AtomicReference<>();
        AtomicReference<List<Volume>> originalVolumes = new AtomicReference<>();
        AtomicReference<List<Container>> originalInitContainers = new AtomicReference<>();

        Checkpoint async = context.checkpoint();
        initialReconcile(context)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                originalPVCs.set(Optional.ofNullable(client.apps().statefulSets().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaClusterName(CLUSTER_NAME)).get())
                        .map(StatefulSet::getSpec)
                        .map(StatefulSetSpec::getVolumeClaimTemplates)
                        .orElse(new ArrayList<>()));
                originalVolumes.set(Optional.ofNullable(client.apps().statefulSets().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaClusterName(CLUSTER_NAME)).get())
                        .map(StatefulSet::getSpec)
                        .map(StatefulSetSpec::getTemplate)
                        .map(PodTemplateSpec::getSpec)
                        .map(PodSpec::getVolumes)
                        .orElse(new ArrayList<>()));
                originalInitContainers.set(Optional.ofNullable(client.apps().statefulSets().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaClusterName(CLUSTER_NAME)).get())
                        .map(StatefulSet::getSpec)
                        .map(StatefulSetSpec::getTemplate)
                        .map(PodTemplateSpec::getSpec)
                        .map(PodSpec::getInitContainers)
                        .orElse(new ArrayList<>()));

                // Update the storage type
                // ephemeral -> persistent
                // or
                // persistent -> ephemeral
                Kafka updatedStorageKafka = null;
                if (kafkaStorage instanceof EphemeralStorage) {
                    updatedStorageKafka = new KafkaBuilder(cluster)
                            .editSpec()
                                .editKafka()
                                    .withNewPersistentClaimStorage()
                                        .withSize("123")
                                    .endPersistentClaimStorage()
                                .endKafka()
                            .endSpec()
                            .build();
                } else if (kafkaStorage instanceof PersistentClaimStorage) {
                    updatedStorageKafka = new KafkaBuilder(cluster)
                            .editSpec()
                                .editKafka()
                                    .withNewEphemeralStorage()
                                    .endEphemeralStorage()
                                .endKafka()
                            .endSpec()
                            .build();
                } else {
                    context.failNow(new Exception("If storage is not ephemeral or persistent something has gone wrong"));
                }
                kafkaAssembly(NAMESPACE, CLUSTER_NAME).patch(updatedStorageKafka);

                LOGGER.info("Updating with changed storage type");
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Check the Volumes and PVCs were not changed
                assertPVCs(context, KafkaCluster.kafkaClusterName(CLUSTER_NAME), originalPVCs.get());
                assertVolumes(context, KafkaCluster.kafkaClusterName(CLUSTER_NAME), originalVolumes.get());
                assertInitContainers(context, KafkaCluster.kafkaClusterName(CLUSTER_NAME), originalInitContainers.get());
                async.flag();
            })));
    }

    private void assertPVCs(VertxTestContext context, String statefulSetName, List<PersistentVolumeClaim> originalPVCs) {
        context.verify(() -> {
            StatefulSet statefulSet = client.apps().statefulSets().inNamespace(NAMESPACE).withName(statefulSetName).get();
            assertThat(statefulSet, is(notNullValue()));
            List<PersistentVolumeClaim> pvcs = statefulSet.getSpec().getVolumeClaimTemplates();
            assertThat(originalPVCs.size(), is(pvcs.size()));
            assertThat(originalPVCs, is(pvcs));
        });

    }

    private void assertVolumes(VertxTestContext context, String statefulSetName, List<Volume> originalVolumes) {
        context.verify(() -> {
            StatefulSet statefulSet = client.apps().statefulSets().inNamespace(NAMESPACE).withName(statefulSetName).get();
            assertThat(statefulSet, is(notNullValue()));
            List<Volume> volumes = statefulSet.getSpec().getTemplate().getSpec().getVolumes();
            assertThat(originalVolumes.size(), is(volumes.size()));
            assertThat(originalVolumes, is(volumes));
        });
    }

    private void assertInitContainers(VertxTestContext context, String statefulSetName, List<Container> originalInitContainers) {
        context.verify(() -> {
            StatefulSet statefulSet = client.apps().statefulSets().inNamespace(NAMESPACE).withName(statefulSetName).get();
            assertThat(statefulSet, is(notNullValue()));
            List<Container> initContainers = statefulSet.getSpec().getTemplate().getSpec().getInitContainers();
            assertThat(originalInitContainers.size(), is(initContainers.size()));
            assertThat(originalInitContainers, is(initContainers));
        });
    }


    /** Test that we can change the deleteClaim flag, and that it's honoured */
    @ParameterizedTest
    @MethodSource("data")
    public void testReconcileUpdatesKafkaWithChangedDeleteClaim(Params params, VertxTestContext context) {
        init(params);
        assumeTrue(kafkaStorage instanceof PersistentClaimStorage, "Kafka delete claims do not apply to non-persistent volumes");

        Map<String, String> kafkaLabels = new HashMap<>();
        kafkaLabels.put(Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND);
        kafkaLabels.put(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME);
        kafkaLabels.put(Labels.STRIMZI_NAME_LABEL, KafkaCluster.kafkaClusterName(CLUSTER_NAME));

        Map<String, String> zkLabels = new HashMap<>();
        zkLabels.put(Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND);
        zkLabels.put(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME);
        zkLabels.put(Labels.STRIMZI_NAME_LABEL, ZookeeperCluster.zookeeperClusterName(CLUSTER_NAME));

        AtomicReference<Set<String>> kafkaPvcs = new AtomicReference<>();
        AtomicReference<Set<String>> zkPvcs = new AtomicReference<>();
        AtomicBoolean originalKafkaDeleteClaim = new AtomicBoolean();


        Checkpoint async = context.checkpoint();

        initialReconcile(context)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                kafkaPvcs.set(client.persistentVolumeClaims().inNamespace(NAMESPACE).withLabels(kafkaLabels).list().getItems()
                        .stream()
                        .map(pvc -> pvc.getMetadata().getName())
                        .collect(Collectors.toSet()));

                zkPvcs.set(client.persistentVolumeClaims().inNamespace(NAMESPACE).withLabels(zkLabels).list().getItems()
                        .stream()
                        .map(pvc -> pvc.getMetadata().getName())
                        .collect(Collectors.toSet()));

                originalKafkaDeleteClaim.set(deleteClaim(kafkaStorage));

                // Try to update the storage class
                Kafka updatedStorageKafka = new KafkaBuilder(cluster).editSpec().editKafka()
                        .withNewPersistentClaimStorage()
                        .withSize("123")
                        .withStorageClass("foo")
                        .withDeleteClaim(!originalKafkaDeleteClaim.get())
                        .endPersistentClaimStorage().endKafka().endSpec().build();
                kafkaAssembly(NAMESPACE, CLUSTER_NAME).patch(updatedStorageKafka);
                LOGGER.info("Updating with changed delete claim");
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // check that the new delete-claim annotation is on the PVCs
                for (String pvcName: kafkaPvcs.get()) {
                    assertThat(client.persistentVolumeClaims().inNamespace(NAMESPACE).withName(pvcName).get()
                                    .getMetadata().getAnnotations(),
                            hasEntry(AbstractModel.ANNO_STRIMZI_IO_DELETE_CLAIM, String.valueOf(!originalKafkaDeleteClaim.get())));
                }
                kafkaAssembly(NAMESPACE, CLUSTER_NAME).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
                LOGGER.info("Reconciling again -> delete");
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> async.flag()));
    }

    /** Create a cluster from a Kafka Cluster CM */
    @ParameterizedTest
    @MethodSource("data")
    public void testReconcileKafkaScaleDown(Params params, VertxTestContext context) {
        init(params);

        assumeTrue(kafkaReplicas > 1, "Skipping scale down test because there's only 1 broker");

        int scaleDownTo = kafkaReplicas - 1;
        // final ordinal will be deleted
        String deletedPod = KafkaCluster.kafkaPodName(CLUSTER_NAME, scaleDownTo);

        AtomicInteger brokersInternalCertsCount = new AtomicInteger();

        Checkpoint async = context.checkpoint();
        initialReconcile(context)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                brokersInternalCertsCount.set(client.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.brokersSecretName(CLUSTER_NAME)).get()
                        .getData()
                        .size());

                assertThat(client.pods().inNamespace(NAMESPACE).withName(deletedPod).get(), is(notNullValue()));

                Kafka scaledDownCluster = new KafkaBuilder(cluster)
                        .editSpec()
                            .editKafka()
                                .withReplicas(scaleDownTo)
                            .endKafka()
                        .endSpec()
                        .build();
                kafkaAssembly(NAMESPACE, CLUSTER_NAME).patch(scaledDownCluster);

                LOGGER.info("Scaling down to {} Kafka pods", scaleDownTo);
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat(client.apps().statefulSets().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaClusterName(CLUSTER_NAME)).get().getSpec().getReplicas(),
                        is(scaleDownTo));
                assertThat("Expected pod " + deletedPod + " to have been deleted",
                        client.pods().inNamespace(NAMESPACE).withName(deletedPod).get(),
                        is(nullValue()));

                // removing one pod, the related private and public keys, keystore and password (4 entries) should not be in the Secrets
                assertThat(client.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.brokersSecretName(CLUSTER_NAME)).get()
                                .getData(),
                        aMapWithSize(brokersInternalCertsCount.get() - 4));

                // TODO assert no rolling update
                async.flag();
            })));
    }

    /** Create a cluster from a Kafka Cluster CM */
    @ParameterizedTest
    @MethodSource("data")
    public void testReconcileKafkaScaleUp(Params params, VertxTestContext context) {
        init(params);

        AtomicInteger brokersInternalCertsCount = new AtomicInteger();

        Checkpoint async = context.checkpoint();
        int scaleUpTo = kafkaReplicas + 1;
        String newPod = KafkaCluster.kafkaPodName(CLUSTER_NAME, kafkaReplicas);

        initialReconcile(context)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                brokersInternalCertsCount.set(client.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.brokersSecretName(CLUSTER_NAME)).get()
                        .getData()
                        .size());

                assertThat(client.pods().inNamespace(NAMESPACE).withName(newPod).get(), is(nullValue()));

                Kafka scaledUpKafka = new KafkaBuilder(cluster)
                        .editSpec()
                            .editKafka()
                                .withReplicas(scaleUpTo)
                            .endKafka()
                        .endSpec()
                        .build();
                kafkaAssembly(NAMESPACE, CLUSTER_NAME).patch(scaledUpKafka);

                LOGGER.info("Scaling up to {} Kafka pods", scaleUpTo);
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat(client.apps().statefulSets().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaClusterName(CLUSTER_NAME)).get().getSpec().getReplicas(),
                        is(scaleUpTo));
                assertThat("Expected pod " + newPod + " to have been created",
                        client.pods().inNamespace(NAMESPACE).withName(newPod).get(),
                        is(notNullValue()));

                // adding one pod, the related private and public keys, keystore and password should be added to the Secrets
                assertThat(client.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.brokersSecretName(CLUSTER_NAME)).get().getData(),
                        aMapWithSize(brokersInternalCertsCount.get() + 4));

                // TODO assert no rolling update
                async.flag();
            })));
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testReconcileResumePartialRoll(Params params, VertxTestContext context) {
        init(params);

        Checkpoint async = context.checkpoint();
        initialReconcile(context)
            .onComplete(v -> async.flag());
    }


    /** Test the ZK version change functions */
    private void reconcileZkVersionChange(VertxTestContext context, String initialKafkaVersion, String changedKafkaVersion, String changedImage) {

        Kafka initialKafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withVersion(initialKafkaVersion)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                    .withNewEntityOperator()
                        .withNewTopicOperator()
                        .endTopicOperator()
                        .withNewUserOperator()
                        .endUserOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        Kafka updatedKafka = new KafkaBuilder(initialKafka)
                .editSpec()
                    .editKafka()
                        .withVersion(changedKafkaVersion)
                    .endKafka()
                .endSpec()
                .build();

        KafkaAssemblyOperator.ReconciliationState initialState = operator.new ReconciliationState(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                updatedKafka);

        Checkpoint async = context.checkpoint();
        initialReconcile(context)
            .onComplete(context.succeeding())
            .compose(v -> initialState.getZookeeperDescription())
            .compose(state -> state.zkVersionChange())
            .onComplete(context.succeeding(state -> context.verify(() -> {
                assertThat(state.zkCluster.getImage(), is(changedImage));
                async.flag();
            })));
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testReconcileZookeeperUpgradeFromPreviousToLatest(Params params, VertxTestContext context) {
        init(params);

        String initialKafkaVersion = KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION;
        String changedKafkaVersion = KafkaVersionTestUtils.LATEST_KAFKA_VERSION;
        String changedImage = KafkaVersionTestUtils.LATEST_KAFKA_IMAGE;

        reconcileZkVersionChange(context, initialKafkaVersion, changedKafkaVersion, changedImage);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testReconcileZookeeperDowngradeFromLatestToPrevious(Params params, VertxTestContext context) {
        init(params);

        String initialKafkaVersion = KafkaVersionTestUtils.LATEST_KAFKA_VERSION;
        String changedKafkaVersion = KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION;
        String changedImage = KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE;

        reconcileZkVersionChange(context, initialKafkaVersion, changedKafkaVersion, changedImage);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testReconcileZookeeperUpgradeFromMinorToPrevious(Params params, VertxTestContext context) {
        init(params);

        String initialKafkaVersion = KafkaVersionTestUtils.PREVIOUS_MINOR_KAFKA_VERSION;
        String changedKafkaVersion = KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION;
        String changedImage = KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE;

        reconcileZkVersionChange(context, initialKafkaVersion, changedKafkaVersion, changedImage);

    }
}
