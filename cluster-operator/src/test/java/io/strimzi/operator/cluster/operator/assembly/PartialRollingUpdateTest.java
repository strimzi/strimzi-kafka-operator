/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaAssemblyList;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.StatefulSetOperator;
import io.strimzi.operator.cluster.operator.resource.ZookeeperLeaderFinder;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.ResourceType;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.test.mockkube.MockKube;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.StringReader;
import java.util.Collections;

import static io.strimzi.test.TestUtils.set;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

@RunWith(VertxUnitRunner.class)
public class PartialRollingUpdateTest {

    private static final Logger LOGGER = LogManager.getLogger(PartialRollingUpdateTest.class);

    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final KafkaVersion.Lookup VERSIONS = new KafkaVersion.Lookup(new StringReader(
            "2.0.0 default 2.0 2.0 1234567890abcdef"),
            singletonMap("2.0.0", "strimzi/kafka:latest-kafka-2.0.0"),
            emptyMap(), emptyMap(), emptyMap()) { };

    private Vertx vertx;
    private Kafka cluster;
    private StatefulSet kafkaSs;
    private StatefulSet zkSs;
    private Pod kafkaPod0;
    private Pod kafkaPod1;
    private Pod kafkaPod2;
    private Pod kafkaPod3;
    private Pod kafkaPod4;
    private KubernetesClient mockClient;
    private KafkaAssemblyOperator kco;
    private Pod zkPod0;
    private Pod zkPod1;
    private Pod zkPod2;
    private Secret clusterCaCert;
    private Secret clusterCaKey;
    private Secret clientsCaCert;
    private Secret clientsCaKey;

    @Before
    public void before(TestContext context) {
        this.vertx = Vertx.vertx();

        this.cluster = new KafkaBuilder()
                .withMetadata(new ObjectMetaBuilder().withName(CLUSTER_NAME)
                .withNamespace(NAMESPACE)
                .build())
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(5)
                        .withNewPersistentClaimStorage()
                            .withSize("123")
                            .withStorageClass("foo")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                        .withMetrics(emptyMap())
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(3)
                        .withNewPersistentClaimStorage()
                            .withSize("123")
                            .withStorageClass("foo")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                        .withMetrics(emptyMap())
                    .endZookeeper()
                    .withNewTopicOperator()
                    .endTopicOperator()
                .endSpec()
                .build();

        CustomResourceDefinition kafkaAssemblyCrd = Crds.kafka();

        KubernetesClient bootstrapClient = new MockKube()
                .withCustomResourceDefinition(kafkaAssemblyCrd, Kafka.class, KafkaAssemblyList.class, DoneableKafka.class)
                .withInitialInstances(Collections.singleton(cluster))
                .end()
                .build();
        ResourceUtils.mockHttpClientForWorkaroundRbac(bootstrapClient);

        ResourceOperatorSupplier supplier = supplier(bootstrapClient);
        KafkaAssemblyOperator kco = new KafkaAssemblyOperator(vertx, true, 2_000,
                new MockCertManager(), supplier, VERSIONS, null);

        LOGGER.info("bootstrap reconciliation");
        Async createAsync = context.async();
        kco.reconcileAssembly(new Reconciliation("test-trigger", ResourceType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
            context.assertTrue(ar.succeeded());
            createAsync.complete();
        });
        createAsync.await();
        LOGGER.info("bootstrap reconciliation complete");

        this.kafkaSs = bootstrapClient.apps().statefulSets().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaClusterName(CLUSTER_NAME)).get();
        this.zkSs = bootstrapClient.apps().statefulSets().inNamespace(NAMESPACE).withName(ZookeeperCluster.zookeeperClusterName(CLUSTER_NAME)).get();
        this.zkPod0 = bootstrapClient.pods().inNamespace(NAMESPACE).withName(ZookeeperCluster.zookeeperPodName(CLUSTER_NAME, 0)).get();
        this.zkPod1 = bootstrapClient.pods().inNamespace(NAMESPACE).withName(ZookeeperCluster.zookeeperPodName(CLUSTER_NAME, 1)).get();
        this.zkPod2 = bootstrapClient.pods().inNamespace(NAMESPACE).withName(ZookeeperCluster.zookeeperPodName(CLUSTER_NAME, 2)).get();
        this.kafkaPod0 = bootstrapClient.pods().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaPodName(CLUSTER_NAME, 0)).get();
        this.kafkaPod1 = bootstrapClient.pods().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaPodName(CLUSTER_NAME, 1)).get();
        this.kafkaPod2 = bootstrapClient.pods().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaPodName(CLUSTER_NAME, 2)).get();
        this.kafkaPod3 = bootstrapClient.pods().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaPodName(CLUSTER_NAME, 3)).get();
        this.kafkaPod4 = bootstrapClient.pods().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaPodName(CLUSTER_NAME, 4)).get();
        this.clusterCaCert = bootstrapClient.secrets().inNamespace(NAMESPACE).withName(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME)).get();
        this.clusterCaKey = bootstrapClient.secrets().inNamespace(NAMESPACE).withName(KafkaResources.clusterCaKeySecretName(CLUSTER_NAME)).get();
        this.clientsCaCert = bootstrapClient.secrets().inNamespace(NAMESPACE).withName(KafkaResources.clientsCaCertificateSecretName(CLUSTER_NAME)).get();
        this.clientsCaKey = bootstrapClient.secrets().inNamespace(NAMESPACE).withName(KafkaResources.clientsCaKeySecretName(CLUSTER_NAME)).get();
    }

    ResourceOperatorSupplier supplier(KubernetesClient bootstrapClient) {
        ZookeeperLeaderFinder leaderFinder = ResourceUtils.zookeeperLeaderFinder(vertx, bootstrapClient);
        return new ResourceOperatorSupplier(vertx, bootstrapClient, leaderFinder, true, 60_000L);
    }

    private void startKube() {
        CustomResourceDefinition kafkaAssemblyCrd = Crds.kafka();

        this.mockClient = new MockKube()
                .withCustomResourceDefinition(kafkaAssemblyCrd, Kafka.class, KafkaAssemblyList.class, DoneableKafka.class)
                .withInitialInstances(Collections.singleton(cluster))
                .end()
                .withInitialStatefulSets(set(zkSs, kafkaSs))
                .withInitialPods(set(zkPod0, zkPod1, zkPod2, kafkaPod0, kafkaPod1, kafkaPod2, kafkaPod3, kafkaPod4))
                .withInitialSecrets(set(clusterCaCert, clusterCaKey, clientsCaCert, clientsCaKey))
                .build();
        ResourceUtils.mockHttpClientForWorkaroundRbac(mockClient);

        ResourceOperatorSupplier supplier = supplier(mockClient);

        this.kco = new KafkaAssemblyOperator(vertx, true, 2_000,
                new MockCertManager(), supplier, VERSIONS, null);
        LOGGER.info("Started test KafkaAssemblyOperator");
    }

    @Test
    public void testReconcileOfPartiallyRolledKafkaCluster(TestContext context) {
        kafkaSs.getSpec().getTemplate().getMetadata().getAnnotations().put(StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, "3");
        kafkaPod0.getMetadata().getAnnotations().put(StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, "3");
        kafkaPod1.getMetadata().getAnnotations().put(StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, "3");
        kafkaPod2.getMetadata().getAnnotations().put(StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, "2");
        kafkaPod3.getMetadata().getAnnotations().put(StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, "1");
        kafkaPod4.getMetadata().getAnnotations().put(StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, "1");

        // Now start the KafkaAssemblyOperator with those pods and that statefulset
        startKube();

        LOGGER.info("Recovery reconciliation");
        Async async = context.async();
        kco.reconcileAssembly(new Reconciliation("test-trigger", ResourceType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
            context.assertTrue(ar.succeeded());
            for (int i = 0; i <= 4; i++) {
                Pod pod = mockClient.pods().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaPodName(CLUSTER_NAME, i)).get();
                String generation = pod.getMetadata().getAnnotations().get(StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION);
                context.assertEquals("3", generation,
                        "Pod " + i + " had unexpected generation " + generation);
            }
            async.complete();
        });
    }

    @Test
    public void testReconcileOfPartiallyRolledZookeeperCluster(TestContext context) {
        zkSs.getSpec().getTemplate().getMetadata().getAnnotations().put(StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, "3");
        zkPod0.getMetadata().getAnnotations().put(StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, "3");
        zkPod1.getMetadata().getAnnotations().put(StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, "2");
        zkPod2.getMetadata().getAnnotations().put(StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, "1");

        // Now start the KafkaAssemblyOperator with those pods and that statefulset
        startKube();

        LOGGER.info("Recovery reconciliation");
        Async async = context.async();
        kco.reconcileAssembly(new Reconciliation("test-trigger", ResourceType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            for (int i = 0; i <= 2; i++) {
                Pod pod = mockClient.pods().inNamespace(NAMESPACE).withName(ZookeeperCluster.zookeeperPodName(CLUSTER_NAME, i)).get();
                String generation = pod.getMetadata().getAnnotations().get(StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION);
                context.assertEquals("3", generation,
                        "Pod " + i + " had unexpected generation " + generation);
            }
            async.complete();
        });
    }

    @Test
    public void testReconcileOfPartiallyRolledClusterForClusterCaCertificate(TestContext context) {
        clusterCaCert.getMetadata().getAnnotations().put(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "3");
        zkPod0.getMetadata().getAnnotations().put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "3");
        zkPod1.getMetadata().getAnnotations().put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "2");
        zkPod2.getMetadata().getAnnotations().put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "1");
        kafkaPod0.getMetadata().getAnnotations().put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "3");
        kafkaPod1.getMetadata().getAnnotations().put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "3");
        kafkaPod2.getMetadata().getAnnotations().put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "2");
        kafkaPod3.getMetadata().getAnnotations().put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "1");
        kafkaPod4.getMetadata().getAnnotations().put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "1");

        // Now start the KafkaAssemblyOperator with those pods and that statefulset
        startKube();

        LOGGER.info("Recovery reconciliation");
        Async async = context.async();
        kco.reconcileAssembly(new Reconciliation("test-trigger", ResourceType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            for (int i = 0; i <= 2; i++) {
                Pod pod = mockClient.pods().inNamespace(NAMESPACE).withName(ZookeeperCluster.zookeeperPodName(CLUSTER_NAME, i)).get();
                String certGeneration = pod.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION);
                context.assertEquals("3", certGeneration,
                        "Pod " + i + " had unexpected cert generation " + certGeneration);
            }
            for (int i = 0; i <= 4; i++) {
                Pod pod = mockClient.pods().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaPodName(CLUSTER_NAME, i)).get();
                String certGeneration = pod.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION);
                context.assertEquals("3", certGeneration,
                        "Pod " + i + " had unexpected cert generation " + certGeneration);
            }
            async.complete();
        });
    }

    @Test
    public void testReconcileOfPartiallyRolledClusterForClientsCaCertificate(TestContext context) {
        clientsCaCert.getMetadata().getAnnotations().put(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "3");
        kafkaPod0.getMetadata().getAnnotations().put(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "3");
        kafkaPod1.getMetadata().getAnnotations().put(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "3");
        kafkaPod2.getMetadata().getAnnotations().put(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "2");
        kafkaPod3.getMetadata().getAnnotations().put(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "1");
        kafkaPod4.getMetadata().getAnnotations().put(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "1");

        // Now start the KafkaAssemblyOperator with those pods and that statefulset
        startKube();

        LOGGER.info("Recovery reconciliation");
        Async async = context.async();
        kco.reconcileAssembly(new Reconciliation("test-trigger", ResourceType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            for (int i = 0; i <= 4; i++) {
                Pod pod = mockClient.pods().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaPodName(CLUSTER_NAME, i)).get();
                String certGeneration = pod.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION);
                context.assertEquals("3", certGeneration,
                        "Pod " + i + " had unexpected cert generation " + certGeneration);
            }
            async.complete();
        });
    }

    @AfterClass
    public static void cleanUp() {
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }
}
