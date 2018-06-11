/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.cluster.Reconciliation;
import io.strimzi.operator.cluster.model.AssemblyType;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.Labels;
import io.strimzi.operator.cluster.model.TopicOperator;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.operator.resource.ConfigMapOperator;
import io.strimzi.operator.cluster.operator.resource.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.KafkaSetOperator;
import io.strimzi.operator.cluster.operator.resource.PvcOperator;
import io.strimzi.operator.cluster.operator.resource.SecretOperator;
import io.strimzi.operator.cluster.operator.resource.ServiceOperator;
import io.strimzi.operator.cluster.operator.resource.StatefulSetOperator;
import io.strimzi.operator.cluster.operator.resource.ZookeeperSetOperator;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;

import static io.strimzi.operator.cluster.ResourceUtils.map;
import static io.strimzi.operator.cluster.ResourceUtils.set;

@RunWith(VertxUnitRunner.class)
public class PartialRollingUpdateTest {

    private static final Logger LOGGER = LogManager.getLogger(PartialRollingUpdateTest.class);

    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-cluster";

    private Vertx vertx;
    private ConfigMap cluster;
    private StatefulSet kafkaSs;
    private StatefulSet zkSs;
    private Pod kafkaPod0;
    private Pod kafkaPod1;
    private Pod kafkaPod2;
    private Pod kafkaPod3;
    private Pod kafkaPod4;
    private ConfigMapOperator cmops;
    private ServiceOperator svcops;
    private KafkaSetOperator ksops;
    private ZookeeperSetOperator zksops;
    private DeploymentOperator depops;
    private PvcOperator pvcops;
    private SecretOperator secretops;
    private KubernetesClient mockClient;
    private KafkaAssemblyOperator kco;
    private Pod zkPod0;
    private Pod zkPod1;
    private Pod zkPod2;

    @Before
    public void before(TestContext context) {
        this.vertx = Vertx.vertx();

        this.cluster = new ConfigMapBuilder()
                .withNewMetadata()
                .withName(CLUSTER_NAME)
                .withNamespace(NAMESPACE)
                .withLabels(Labels.forKind("cluster").withType(AssemblyType.KAFKA).toMap())
                .endMetadata()
                .withData(map(KafkaCluster.KEY_REPLICAS, String.valueOf(5),
                        KafkaCluster.KEY_STORAGE, "{\"type\": \"persistent-claim\", " +
                                "\"size\": \"123\", " +
                                "\"class\": \"foo\"," +
                                "\"delete-claim\": true}",
                        KafkaCluster.KEY_METRICS_CONFIG, "{}",
                        ZookeeperCluster.KEY_REPLICAS, String.valueOf(3),
                        ZookeeperCluster.KEY_STORAGE, "{\"type\": \"persistent-claim\", " +
                                "\"size\": \"123\", " +
                                "\"class\": \"foo\"," +
                                "\"delete-claim\": true}",
                        ZookeeperCluster.KEY_METRICS_CONFIG, "{}",
                        TopicOperator.KEY_CONFIG, "{}"))
                .build();
        KubernetesClient bootstrapClient = new MockKube().withInitialCms(Collections.singleton(cluster)).build();
        cmops = new ConfigMapOperator(vertx, bootstrapClient);
        svcops = new ServiceOperator(vertx, bootstrapClient);
        ksops = new KafkaSetOperator(vertx, bootstrapClient, 60_000L);
        zksops = new ZookeeperSetOperator(vertx, bootstrapClient, 60_000L);
        depops = new DeploymentOperator(vertx, bootstrapClient);
        pvcops = new PvcOperator(vertx, bootstrapClient);
        secretops = new SecretOperator(vertx, bootstrapClient);
        KafkaAssemblyOperator kco = new KafkaAssemblyOperator(vertx, true, 2_000,
                cmops, svcops, zksops, ksops, pvcops, depops, secretops);

        LOGGER.info("bootstrap reconciliation");
        Async createAsync = context.async();
        kco.reconcileAssembly(new Reconciliation("test-trigger", AssemblyType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
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

    }

    private void startKube() {
        this.mockClient = new MockKube()
                .withInitialCms(Collections.singleton(cluster))
                .withInitialStatefulSets(set(zkSs, kafkaSs))
                .withInitialPods(set(zkPod0, zkPod1, zkPod2, kafkaPod0, kafkaPod1, kafkaPod2, kafkaPod3, kafkaPod4))
                .build();
        cmops = new ConfigMapOperator(vertx, mockClient);
        svcops = new ServiceOperator(vertx, mockClient);
        ksops = new KafkaSetOperator(vertx, mockClient, 60_000L);
        zksops = new ZookeeperSetOperator(vertx, mockClient, 60_000L);
        depops = new DeploymentOperator(vertx, mockClient);
        pvcops = new PvcOperator(vertx, mockClient);
        secretops = new SecretOperator(vertx, mockClient);

        this.kco = new KafkaAssemblyOperator(vertx, true, 2_000,
                cmops, svcops, zksops, ksops, pvcops, depops, secretops);
        LOGGER.info("Started test KafkaAssemblyOperator");
    }

    @Test
    public void testReconcileOfPartiallyRolledKafkaCluster(TestContext context) {
        kafkaSs.getSpec().getTemplate().getMetadata().getAnnotations().put(StatefulSetOperator.ANNOTATION_GENERATION, "3");
        zkPod0.getMetadata().getAnnotations().put(StatefulSetOperator.ANNOTATION_GENERATION, "3");
        kafkaPod0.getMetadata().getAnnotations().put(StatefulSetOperator.ANNOTATION_GENERATION, "3");
        kafkaPod1.getMetadata().getAnnotations().put(StatefulSetOperator.ANNOTATION_GENERATION, "3");
        kafkaPod2.getMetadata().getAnnotations().put(StatefulSetOperator.ANNOTATION_GENERATION, "2");
        kafkaPod3.getMetadata().getAnnotations().put(StatefulSetOperator.ANNOTATION_GENERATION, "1");
        kafkaPod4.getMetadata().getAnnotations().put(StatefulSetOperator.ANNOTATION_GENERATION, "1");

        // Now start the KafkaAssemblyOperator with those pods and that statefulset
        startKube();

        LOGGER.info("Recovery reconciliation");
        Async async = context.async();
        kco.reconcileAssembly(new Reconciliation("test-trigger", AssemblyType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
            context.assertTrue(ar.succeeded());
            for (int i = 0; i <= 4; i++) {
                Pod pod = mockClient.pods().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaPodName(CLUSTER_NAME, i)).get();
                String generation = pod.getMetadata().getAnnotations().get(StatefulSetOperator.ANNOTATION_GENERATION);
                context.assertEquals("3", generation,
                        "Pod " + i + " had unexpected generation " + generation);
            }
            async.complete();
        });
    }

    @Test
    public void testReconcileOfPartiallyRolledZookeeperCluster(TestContext context) {
        zkSs.getSpec().getTemplate().getMetadata().getAnnotations().put(StatefulSetOperator.ANNOTATION_GENERATION, "3");
        zkPod0.getMetadata().getAnnotations().put(StatefulSetOperator.ANNOTATION_GENERATION, "3");
        zkPod1.getMetadata().getAnnotations().put(StatefulSetOperator.ANNOTATION_GENERATION, "2");
        zkPod2.getMetadata().getAnnotations().put(StatefulSetOperator.ANNOTATION_GENERATION, "1");

        // Now start the KafkaAssemblyOperator with those pods and that statefulset
        startKube();

        LOGGER.info("Recovery reconciliation");
        Async async = context.async();
        kco.reconcileAssembly(new Reconciliation("test-trigger", AssemblyType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
            context.assertTrue(ar.succeeded());
            for (int i = 0; i <= 2; i++) {
                Pod pod = mockClient.pods().inNamespace(NAMESPACE).withName(ZookeeperCluster.zookeeperPodName(CLUSTER_NAME, i)).get();
                String generation = pod.getMetadata().getAnnotations().get(StatefulSetOperator.ANNOTATION_GENERATION);
                context.assertEquals("3", generation,
                        "Pod " + i + " had unexpected generation " + generation);
            }
            async.complete();
        });
    }

}
