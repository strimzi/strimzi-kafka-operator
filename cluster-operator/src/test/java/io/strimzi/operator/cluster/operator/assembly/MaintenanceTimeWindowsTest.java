/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaAssemblyList;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.model.EntityOperatorSpec;
import io.strimzi.api.kafka.model.EntityOperatorSpecBuilder;
import io.strimzi.api.kafka.model.EntityTopicOperatorSpec;
import io.strimzi.api.kafka.model.EntityTopicOperatorSpecBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.TopicOperatorSpec;
import io.strimzi.api.kafka.model.TopicOperatorSpecBuilder;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.ClientsCa;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.EntityOperator;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.TopicOperator;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.ResourceType;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.StringReader;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

@RunWith(VertxUnitRunner.class)
public class MaintenanceTimeWindowsTest {

    private static final String NAMESPACE = "test-maintenance-time-windows";
    private static final String NAME = "my-kafka";
    private static final KafkaVersion.Lookup VERSIONS = new KafkaVersion.Lookup(new StringReader(
            "2.0.0 default 2.0 2.0 1234567890abcdef"),
            singletonMap("2.0.0", "strimzi/kafka:latest-kafka-2.0.0"),
            emptyMap(), emptyMap(), emptyMap()) { };

    private Vertx vertx;
    private Kafka kafka;
    private KubernetesClient mockClient;
    private KafkaAssemblyOperator.ReconciliationState reconciliationState;

    private Secret clusterCaSecret;
    private Secret clientsCaSecret;

    private void initMockClient() {

        // setting up the Kafka CRD
        CustomResourceDefinition kafkaAssemblyCrd = Crds.kafka();

        // setting up a mock Kubernetes client
        this.mockClient = new MockKube()
                .withCustomResourceDefinition(kafkaAssemblyCrd, Kafka.class, KafkaAssemblyList.class, DoneableKafka.class)
                .withInitialInstances(Collections.singleton(this.kafka))
                .end()
                .build();

        this.clusterCaSecret = new SecretBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(KafkaResources.clusterCaCertificateSecretName(NAME))
                    .withAnnotations(Collections.singletonMap(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "1"))
                .endMetadata()
                .build();
        this.clientsCaSecret = new SecretBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(KafkaResources.clientsCaCertificateSecretName(NAME))
                    .withAnnotations(Collections.singletonMap(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "1"))
                .endMetadata()
                .build();
        this.mockClient.secrets().inNamespace(NAMESPACE).withName(KafkaResources.clusterCaCertificateSecretName(NAME)).create(this.clusterCaSecret);
        this.mockClient.secrets().inNamespace(NAMESPACE).withName(KafkaResources.clientsCaCertificateSecretName(NAME)).create(this.clientsCaSecret);

        this.vertx = Vertx.vertx();

        // creating the Kafka operator
        ResourceOperatorSupplier ros =
                new ResourceOperatorSupplier(this.vertx, this.mockClient, false, 60_000L);

        KafkaAssemblyOperator kao = new KafkaAssemblyOperator(this.vertx, false, 2_000, null, ros, VERSIONS, null);

        Reconciliation reconciliation = new Reconciliation("test-trigger", ResourceType.KAFKA, NAMESPACE, NAME);

        this.reconciliationState = kao.new ReconciliationState(reconciliation, kafka);
    }

    private void init(List<String> maintenanceTimeWindows) {

        EntityTopicOperatorSpec entityTopicOperatorSpec = new EntityTopicOperatorSpecBuilder()
                .build();

        EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder()
                .withTopicOperator(entityTopicOperatorSpec)
                .build();

        this.kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(NAME)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(1)
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(1)
                    .endZookeeper()
                    .withEntityOperator(entityOperatorSpec)
                    .withMaintenanceTimeWindows(maintenanceTimeWindows)
                .endSpec()
                .build();

        initMockClient();
    }

    @Deprecated
    private void initWithTopicOperator(List<String> maintenanceTimeWindows) {

        TopicOperatorSpec topicOperatorSpec = new TopicOperatorSpecBuilder()
                .build();

        this.kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(NAME)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(1)
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(1)
                    .endZookeeper()
                    .withTopicOperator(topicOperatorSpec)
                    .withMaintenanceTimeWindows(maintenanceTimeWindows)
                .endSpec()
                .build();

        initMockClient();
    }

    @Test
    public void testZkRollingUpdateMaintenanceSatisfied(TestContext context) {

        Async async = context.async();

        doZkRollingUpdate(Collections.singletonList("* * 8-10 * * ?"),
            () -> Date.from(LocalDateTime.of(2018, 11, 26, 9, 00, 0).atZone(ZoneId.of("GMT")).toInstant()),
            r -> {
                String generation = getClusterCaGenerationPod(ZookeeperCluster.zookeeperPodName(NAME, 0));
                context.assertEquals("1", generation, "Pod had unexpected generation " + generation);
                async.complete();
            });

        async.await();
    }

    @Test
    public void testZkRollingUpdateMaintenanceNotSatisfied(TestContext context) {

        Async async = context.async();

        doZkRollingUpdate(Collections.singletonList("* * 8-10 * * ?"),
            () -> Date.from(LocalDateTime.of(2018, 11, 26, 11, 00, 0).atZone(ZoneId.of("GMT")).toInstant()),
            r -> {
                String generation = getClusterCaGenerationPod(ZookeeperCluster.zookeeperPodName(NAME, 0));
                context.assertEquals("0", generation, "Pod had unexpected generation " + generation);
                async.complete();
            });

        async.await();
    }

    @Test
    public void testZkRollingUpdateNoMaintenance(TestContext context) {

        Async async = context.async();

        doZkRollingUpdate(null,
            () -> Date.from(LocalDateTime.of(2018, 11, 26, 11, 00, 0).atZone(ZoneId.of("GMT")).toInstant()),
            r -> {
                String generation = getClusterCaGenerationPod(ZookeeperCluster.zookeeperPodName(NAME, 0));
                context.assertEquals("1", generation, "Pod had unexpected generation " + generation);
                async.complete();
            });

        async.await();
    }

    @Test
    public void testZkRollingUpdateMoreMaintenanceWindowsSatisfied(TestContext context) {

        Async async = context.async();

        doZkRollingUpdate(Arrays.asList("* * 8-10 * * ?", "* * 14-15 * * ?"),
            () -> Date.from(LocalDateTime.of(2018, 11, 26, 9, 00, 0).atZone(ZoneId.of("GMT")).toInstant()),
            r -> {
                String generation = getClusterCaGenerationPod(ZookeeperCluster.zookeeperPodName(NAME, 0));
                context.assertEquals("1", generation, "Pod had unexpected generation " + generation);
                async.complete();
            });

        async.await();
    }

    @Test
    public void testKafkaRollingUpdateMaintenanceSatisfied(TestContext context) {

        Async async = context.async();

        doKafkaRollingUpdate(Collections.singletonList("* * 8-10 * * ?"),
            () -> Date.from(LocalDateTime.of(2018, 11, 26, 9, 00, 0).atZone(ZoneId.of("GMT")).toInstant()),
            r -> {
                String generation = getClusterCaGenerationPod(KafkaCluster.kafkaPodName(NAME, 0));
                context.assertEquals("1", generation, "Pod had unexpected generation " + generation);
                async.complete();
            });

        async.await();
    }

    @Test
    public void testKafkaRollingUpdateMaintenanceNotSatisfied(TestContext context) {

        Async async = context.async();

        doKafkaRollingUpdate(Collections.singletonList("* * 8-10 * * ?"),
            () -> Date.from(LocalDateTime.of(2018, 11, 26, 11, 00, 0).atZone(ZoneId.of("GMT")).toInstant()),
            r -> {
                String generation = getClusterCaGenerationPod(KafkaCluster.kafkaPodName(NAME, 0));
                context.assertEquals("0", generation, "Pod had unexpected generation " + generation);
                async.complete();
            });

        async.await();
    }

    @Test
    public void testKafkaRollingUpdateNoMaintenance(TestContext context) {

        Async async = context.async();

        doKafkaRollingUpdate(null,
            () -> Date.from(LocalDateTime.of(2018, 11, 26, 11, 00, 0).atZone(ZoneId.of("GMT")).toInstant()),
            r -> {
                String generation = getClusterCaGenerationPod(KafkaCluster.kafkaPodName(NAME, 0));
                context.assertEquals("1", generation, "Pod had unexpected generation " + generation);
                async.complete();
            });

        async.await();
    }

    @Test
    public void testKafkaRollingUpdateMaintenanceSatisfiedDifferentTz(TestContext context) {

        Async async = context.async();

        doKafkaRollingUpdate(Collections.singletonList("* * 14-15 * * ?"),
            () -> Date.from(LocalDateTime.of(2018, 11, 26, 9, 00, 0).atZone(ZoneId.of("Pacific/Easter")).toInstant()),
            r -> {
                String generation = getClusterCaGenerationPod(KafkaCluster.kafkaPodName(NAME, 0));
                context.assertEquals("1", generation, "Pod had unexpected generation " + generation);
                async.complete();
            });

        async.await();
    }

    @Test
    public void testEntityOperatorRollingUpdateMaintenanceSatisfied(TestContext context) {

        Async async = context.async();

        doEntityOperatorRollingUpdate(Collections.singletonList("* * 8-10 * * ?"),
            () -> Date.from(LocalDateTime.of(2018, 11, 26, 9, 00, 0).atZone(ZoneId.of("GMT")).toInstant()),
            r -> {
                String generation = getClusterCaGenerationPod(this.mockClient.pods().inNamespace(NAMESPACE).list().getItems().get(0));
                context.assertEquals("1", generation, "Pod had unexpected generation " + generation);
                async.complete();
            });

        async.await();
    }

    @Test
    public void testEntityOperatorRollingUpdateMaintenanceNotSatisfied(TestContext context) {

        Async async = context.async();

        doEntityOperatorRollingUpdate(Collections.singletonList("* * 8-10 * * ?"),
            () -> Date.from(LocalDateTime.of(2018, 11, 26, 11, 00, 0).atZone(ZoneId.of("GMT")).toInstant()),
            r -> {
                String generation = getClusterCaGenerationPod(this.mockClient.pods().inNamespace(NAMESPACE).list().getItems().get(0));
                context.assertEquals("0", generation, "Pod had unexpected generation " + generation);
                async.complete();
            });

        async.await();
    }

    @Test
    public void testEntityOperatorRollingUpdateNoMaintenance(TestContext context) {

        Async async = context.async();

        doEntityOperatorRollingUpdate(null,
            () -> Date.from(LocalDateTime.of(2018, 11, 26, 11, 00, 0).atZone(ZoneId.of("GMT")).toInstant()),
            r -> {
                String generation = getClusterCaGenerationPod(this.mockClient.pods().inNamespace(NAMESPACE).list().getItems().get(0));
                context.assertEquals("1", generation, "Pod had unexpected generation " + generation);
                async.complete();
            });

        async.await();
    }

    @Test
    public void testEntityOperatorRollingUpdateMaintenanceSatisfiedDifferentTz(TestContext context) {

        Async async = context.async();

        doEntityOperatorRollingUpdate(Collections.singletonList("* * 14-15 * * ?"),
            () -> Date.from(LocalDateTime.of(2018, 11, 26, 9, 00, 0).atZone(ZoneId.of("Pacific/Easter")).toInstant()),
            r -> {
                String generation = getClusterCaGenerationPod(this.mockClient.pods().inNamespace(NAMESPACE).list().getItems().get(0));
                context.assertEquals("1", generation, "Pod had unexpected generation " + generation);
                async.complete();
            });

        async.await();
    }

    @Test
    public void testTopicOperatorRollingUpdateMaintenanceSatisfied(TestContext context) {

        Async async = context.async();

        doTopicOperatorRollingUpdate(Collections.singletonList("* * 8-10 * * ?"),
            () -> Date.from(LocalDateTime.of(2018, 11, 26, 9, 00, 0).atZone(ZoneId.of("GMT")).toInstant()),
            r -> {
                String generation = getClusterCaGenerationPod(this.mockClient.pods().inNamespace(NAMESPACE).list().getItems().get(0));
                context.assertEquals("1", generation, "Pod had unexpected generation " + generation);
                async.complete();
            });

        async.await();
    }

    @Test
    public void testTopicOperatorRollingUpdateMaintenanceNotSatisfied(TestContext context) {

        Async async = context.async();

        doTopicOperatorRollingUpdate(Collections.singletonList("* * 8-10 * * ?"),
            () -> Date.from(LocalDateTime.of(2018, 11, 26, 11, 00, 0).atZone(ZoneId.of("GMT")).toInstant()),
            r -> {
                String generation = getClusterCaGenerationPod(this.mockClient.pods().inNamespace(NAMESPACE).list().getItems().get(0));
                context.assertEquals("0", generation, "Pod had unexpected generation " + generation);
                async.complete();
            });

        async.await();
    }

    @Test
    public void testTopicOperatorRollingUpdateNoMaintenance(TestContext context) {

        Async async = context.async();

        doTopicOperatorRollingUpdate(null,
            () -> Date.from(LocalDateTime.of(2018, 11, 26, 11, 00, 0).atZone(ZoneId.of("GMT")).toInstant()),
            r -> {
                String generation = getClusterCaGenerationPod(this.mockClient.pods().inNamespace(NAMESPACE).list().getItems().get(0));
                context.assertEquals("1", generation, "Pod had unexpected generation " + generation);
                async.complete();
            });

        async.await();
    }

    @Test
    public void testTopicOperatorRollingUpdateMoreMaintenanceWindowsSatisfied(TestContext context) {

        Async async = context.async();

        doTopicOperatorRollingUpdate(Arrays.asList("* * 8-10 * * ?", "* * 14-15 * * ?"),
            () -> Date.from(LocalDateTime.of(2018, 11, 26, 9, 00, 0).atZone(ZoneId.of("GMT")).toInstant()),
            r -> {
                String generation = getClusterCaGenerationPod(this.mockClient.pods().inNamespace(NAMESPACE).list().getItems().get(0));
                context.assertEquals("1", generation, "Pod had unexpected generation " + generation);
                async.complete();
            });

        async.await();
    }

    private String getClusterCaGenerationPod(String podName) {
        Pod pod = this.mockClient.pods().inNamespace(NAMESPACE).withName(podName).get();
        return pod.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION);
    }

    private String getClusterCaGenerationPod(Pod pod) {
        return pod.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION);
    }

    private void doZkRollingUpdate(List<String> maintenanceTimeWindows, Supplier<Date> dateSupplier,
                                   Handler<AsyncResult<KafkaAssemblyOperator.ReconciliationState>> handler) {

        this.init(maintenanceTimeWindows);

        StatefulSet zkSS = ZookeeperCluster.fromCrd(this.kafka, VERSIONS).generateStatefulSet(false, null);
        zkSS.getSpec().getTemplate().getMetadata().getAnnotations().put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0");
        this.mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(ZookeeperCluster.zookeeperClusterName(NAME)).create(zkSS);

        this.reconciliationState.zkDiffs = ReconcileResult.created(zkSS);
        this.reconciliationState.clusterCa = new ClusterCa(null, null, this.clusterCaSecret, null) {

            @Override
            public boolean certRenewed() {
                return true;
            }
        };

        StatefulSet z = this.mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(ZookeeperCluster.zookeeperClusterName(NAME)).get();
        z.getSpec().getTemplate().getMetadata().getAnnotations().put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "1");
        this.mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(ZookeeperCluster.zookeeperClusterName(NAME)).patch(z);

        this.reconciliationState.zkRollingUpdate(dateSupplier).setHandler(handler);
    }

    private void doKafkaRollingUpdate(List<String> maintenanceTimeWindows, Supplier<Date> dateSupplier,
                                      Handler<AsyncResult<KafkaAssemblyOperator.ReconciliationState>> handler) {

        this.init(maintenanceTimeWindows);

        StatefulSet kafkaSS = KafkaCluster.fromCrd(this.kafka, VERSIONS).generateStatefulSet(false, null);
        kafkaSS.getSpec().getTemplate().getMetadata().getAnnotations().put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0");
        kafkaSS.getSpec().getTemplate().getMetadata().getAnnotations().put(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0");
        this.mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaClusterName(NAME)).create(kafkaSS);

        this.reconciliationState.kafkaDiffs = ReconcileResult.created(kafkaSS);
        this.reconciliationState.clusterCa = new ClusterCa(null, null, this.clusterCaSecret, null) {

            @Override
            public boolean certRenewed() {
                return true;
            }
        };
        this.reconciliationState.clientsCa = new ClientsCa(null, null, this.clientsCaSecret, null, null, 0, 0, true, null) {

            @Override
            public boolean certRenewed() {
                return false;
            }
        };

        StatefulSet k = this.mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaClusterName(NAME)).get();
        k.getSpec().getTemplate().getMetadata().getAnnotations().put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "1");
        k.getSpec().getTemplate().getMetadata().getAnnotations().put(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0");
        this.mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaClusterName(NAME)).patch(k);

        this.reconciliationState.kafkaRollingUpdate(dateSupplier).setHandler(handler);
    }

    private void doEntityOperatorRollingUpdate(List<String> maintenanceTimeWindows, Supplier<Date> dateSupplier,
                                               Handler<AsyncResult<KafkaAssemblyOperator.ReconciliationState>> handler) {

        this.init(maintenanceTimeWindows);

        EntityOperator eo = EntityOperator.fromCrd(this.kafka);
        Deployment eoDep = eo.generateDeployment(false, Collections.EMPTY_MAP, null);
        eoDep.getSpec().getTemplate().getMetadata().getAnnotations().put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0");
        this.mockClient.extensions().deployments().inNamespace(NAMESPACE).withName(EntityOperator.entityOperatorName(NAME)).create(eoDep);

        this.reconciliationState.entityOperator = eo;
        this.reconciliationState.eoDeployment = eoDep;
        this.reconciliationState.clusterCa = new ClusterCa(null, null, this.clusterCaSecret, null) {

            @Override
            public boolean certRenewed() {
                return true;
            }
        };
        this.reconciliationState.clientsCa = new ClientsCa(null, null, this.clientsCaSecret, null, null, 1, 1, false, null) {

            @Override
            public boolean certRenewed() {
                return true;
            }
        };

        this.reconciliationState.entityOperatorDeployment(dateSupplier).setHandler(handler);
    }

    @Deprecated
    private void doTopicOperatorRollingUpdate(List<String> maintenanceTimeWindows, Supplier<Date> dateSupplier,
                                              Handler<AsyncResult<KafkaAssemblyOperator.ReconciliationState>> handler) {

        this.initWithTopicOperator(maintenanceTimeWindows);

        TopicOperator to = TopicOperator.fromCrd(this.kafka);
        Deployment toDep = to.generateDeployment(false, null);
        toDep.getSpec().getTemplate().getMetadata().getAnnotations().put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0");
        this.mockClient.extensions().deployments().inNamespace(NAMESPACE).withName(TopicOperator.topicOperatorName(NAME)).create(toDep);

        this.reconciliationState.topicOperator = to;
        this.reconciliationState.toDeployment = toDep;
        this.reconciliationState.clusterCa = new ClusterCa(null, null, this.clusterCaSecret, null) {

            @Override
            public boolean certRenewed() {
                return true;
            }
        };

        this.reconciliationState.topicOperatorDeployment(dateSupplier).setHandler(handler);
    }
}
