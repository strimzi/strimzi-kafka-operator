/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.strimzi.controller.cluster.ResourceUtils;
import io.strimzi.controller.cluster.operations.resource.ConfigMapOperations;
import io.strimzi.controller.cluster.operations.resource.DeploymentOperations;
import io.strimzi.controller.cluster.operations.resource.ServiceOperations;
import io.strimzi.controller.cluster.resources.KafkaConnectCluster;
import io.strimzi.controller.cluster.resources.Labels;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class KafkaConnectClusterOperationsTest {

    protected static Vertx vertx;

    @BeforeClass
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterClass
    public static void after() {
        vertx.close();
    }

    @Test
    public void testCreateCluster(TestContext context) {
        ConfigMapOperations mockCmOps = mock(ConfigMapOperations.class);
        ServiceOperations mockServiceOps = mock(ServiceOperations.class);
        DeploymentOperations mockDcOps = mock(DeploymentOperations.class);

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        ConfigMap clusterCm = ResourceUtils.createEmptyKafkaConnectClusterConfigMap(clusterCmNamespace, clusterCmName);
        when(mockCmOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);

        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.create(serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.create(dcCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectClusterOperations ops = new KafkaConnectClusterOperations(vertx, true,
                mockCmOps, mockDcOps, mockServiceOps);

        KafkaConnectCluster connect = KafkaConnectCluster.fromConfigMap(clusterCm);

        Async async = context.async();
        ops.create(clusterCmNamespace, clusterCmName, createResult -> {
            context.assertTrue(createResult.succeeded());

            // Vertify service
            List<Service> capturedServices = serviceCaptor.getAllValues();
            context.assertEquals(1, capturedServices.size());
            Service service = capturedServices.get(0);
            context.assertEquals(connect.getName(), service.getMetadata().getName());
            context.assertEquals(connect.generateService(), service, "Services are not equal");

            // Verify Deployment
            List<Deployment> capturedDc = dcCaptor.getAllValues();
            context.assertEquals(1, capturedDc.size());
            Deployment dc = capturedDc.get(0);
            context.assertEquals(connect.getName(), dc.getMetadata().getName());
            context.assertEquals(connect.generateDeployment(), dc, "Deployments are not equal");

            async.complete();
        });
    }

    @Test
    public void testUpdateClusterNoDiff(TestContext context) {
        ConfigMapOperations mockCmOps = mock(ConfigMapOperations.class);
        ServiceOperations mockServiceOps = mock(ServiceOperations.class);
        DeploymentOperations mockDcOps = mock(DeploymentOperations.class);

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        ConfigMap clusterCm = ResourceUtils.createEmptyKafkaConnectClusterConfigMap(clusterCmNamespace, clusterCmName);
        KafkaConnectCluster connect = KafkaConnectCluster.fromConfigMap(clusterCm);
        when(mockCmOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockServiceOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateDeployment());

        ArgumentCaptor<String> serviceNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.patch(serviceNamespaceCaptor.capture(), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.patch(dcNamespaceCaptor.capture(), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleUpNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(dcScaleUpNamespaceCaptor.capture(), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(dcScaleDownNamespaceCaptor.capture(), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectClusterOperations ops = new KafkaConnectClusterOperations(vertx, true,
                mockCmOps, mockDcOps, mockServiceOps);

        Async async = context.async();
        ops.update(clusterCmNamespace, clusterCmName, createResult -> {
            context.assertTrue(createResult.succeeded());

            // Vertify service
            List<Service> capturedServices = serviceCaptor.getAllValues();
            context.assertEquals(0, capturedServices.size());

            // Verify Deployment Config
            List<Deployment> capturedDc = dcCaptor.getAllValues();
            context.assertEquals(0, capturedDc.size());

            // Verify scaleDown / scaleUp were not called
            context.assertEquals(0, dcScaleDownNameCaptor.getAllValues().size());
            context.assertEquals(0, dcScaleUpNameCaptor.getAllValues().size());

            async.complete();
        });
    }

    @Test
    public void testUpdateCluster(TestContext context) {
        ConfigMapOperations mockCmOps = mock(ConfigMapOperations.class);
        ServiceOperations mockServiceOps = mock(ServiceOperations.class);
        DeploymentOperations mockDcOps = mock(DeploymentOperations.class);

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        ConfigMap clusterCm = ResourceUtils.createEmptyKafkaConnectClusterConfigMap(clusterCmNamespace, clusterCmName);
        KafkaConnectCluster connect = KafkaConnectCluster.fromConfigMap(clusterCm);
        clusterCm.getData().put("image", "some/different:image"); // Change the image to generate some diff

        when(mockCmOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockServiceOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateDeployment());

        ArgumentCaptor<String> serviceNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.patch(serviceNamespaceCaptor.capture(), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.patch(dcNamespaceCaptor.capture(), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleUpNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(dcScaleUpNamespaceCaptor.capture(), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(dcScaleDownNamespaceCaptor.capture(), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectClusterOperations ops = new KafkaConnectClusterOperations(vertx, true,
                mockCmOps, mockDcOps, mockServiceOps);

        Async async = context.async();
        ops.update(clusterCmNamespace, clusterCmName, createResult -> {
            context.assertTrue(createResult.succeeded());

            KafkaConnectCluster compareTo = KafkaConnectCluster.fromConfigMap(clusterCm);

            // Vertify service
            List<Service> capturedServices = serviceCaptor.getAllValues();
            context.assertEquals(1, capturedServices.size());
            Service service = capturedServices.get(0);
            context.assertEquals(compareTo.getName(), service.getMetadata().getName());
            context.assertEquals(compareTo.generateService(), service, "Services are not equal");

            // Verify Deployment
            List<Deployment> capturedDc = dcCaptor.getAllValues();
            context.assertEquals(1, capturedDc.size());
            Deployment dc = capturedDc.get(0);
            context.assertEquals(compareTo.getName(), dc.getMetadata().getName());
            context.assertEquals(compareTo.generateDeployment(), dc, "Deployments are not equal");

            // Verify scaleDown / scaleUp were not called
            context.assertEquals(0, dcScaleDownNameCaptor.getAllValues().size());
            context.assertEquals(0, dcScaleUpNameCaptor.getAllValues().size());

            async.complete();
        });
    }

    @Test
    public void testUpdateClusterFailure(TestContext context) {
        ConfigMapOperations mockCmOps = mock(ConfigMapOperations.class);
        ServiceOperations mockServiceOps = mock(ServiceOperations.class);
        DeploymentOperations mockDcOps = mock(DeploymentOperations.class);

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        ConfigMap clusterCm = ResourceUtils.createEmptyKafkaConnectClusterConfigMap(clusterCmNamespace, clusterCmName);
        KafkaConnectCluster connect = KafkaConnectCluster.fromConfigMap(clusterCm);
        clusterCm.getData().put("image", "some/different:image"); // Change the image to generate some diff

        when(mockCmOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockServiceOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateDeployment());

        ArgumentCaptor<String> serviceNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.patch(serviceNamespaceCaptor.capture(), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.patch(dcNamespaceCaptor.capture(), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.failedFuture("Failed"));

        ArgumentCaptor<String> dcScaleUpNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(dcScaleUpNamespaceCaptor.capture(), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(dcScaleDownNamespaceCaptor.capture(), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectClusterOperations ops = new KafkaConnectClusterOperations(vertx, true,
                mockCmOps, mockDcOps, mockServiceOps);

        Async async = context.async();
        ops.update(clusterCmNamespace, clusterCmName, createResult -> {
            context.assertFalse(createResult.succeeded());

            async.complete();
        });
    }

    @Test
    public void testUpdateClusterScaleUp(TestContext context) {
        String newReplicas = "4";

        ConfigMapOperations mockCmOps = mock(ConfigMapOperations.class);
        ServiceOperations mockServiceOps = mock(ServiceOperations.class);
        DeploymentOperations mockDcOps = mock(DeploymentOperations.class);

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        ConfigMap clusterCm = ResourceUtils.createEmptyKafkaConnectClusterConfigMap(clusterCmNamespace, clusterCmName);
        KafkaConnectCluster connect = KafkaConnectCluster.fromConfigMap(clusterCm);
        clusterCm.getData().put(KafkaConnectCluster.KEY_REPLICAS, newReplicas); // Change replicas to create ScaleUp

        when(mockCmOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockServiceOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateDeployment());

        ArgumentCaptor<String> serviceNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.patch(serviceNamespaceCaptor.capture(), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.patch(dcNamespaceCaptor.capture(), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleUpNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(dcScaleUpNamespaceCaptor.capture(), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(dcScaleDownNamespaceCaptor.capture(), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectClusterOperations ops = new KafkaConnectClusterOperations(vertx, true,
                mockCmOps, mockDcOps, mockServiceOps);

        Async async = context.async();
        ops.update(clusterCmNamespace, clusterCmName, createResult -> {
            context.assertTrue(createResult.succeeded());

            // Vertify service
            List<Service> capturedServices = serviceCaptor.getAllValues();
            context.assertEquals(0, capturedServices.size());

            // Verify Deployment
            List<Deployment> capturedDc = dcCaptor.getAllValues();
            context.assertEquals(0, capturedDc.size());

            // Verify ScaleUp
            context.assertEquals(1, dcScaleUpNameCaptor.getAllValues().size());
            context.assertEquals(clusterCmNamespace, dcScaleUpNamespaceCaptor.getValue());
            context.assertEquals(connect.getName(), dcScaleUpNameCaptor.getValue());
            context.assertEquals(newReplicas, dcScaleUpReplicasCaptor.getValue().toString());

            // Verify scaleDown was not called
            context.assertEquals(0, dcScaleDownNameCaptor.getAllValues().size());

            async.complete();
        });
    }

    @Test
    public void testUpdateClusterScaleDown(TestContext context) {
        String newReplicas = "2";

        ConfigMapOperations mockCmOps = mock(ConfigMapOperations.class);
        ServiceOperations mockServiceOps = mock(ServiceOperations.class);
        DeploymentOperations mockDcOps = mock(DeploymentOperations.class);

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        ConfigMap clusterCm = ResourceUtils.createEmptyKafkaConnectClusterConfigMap(clusterCmNamespace, clusterCmName);
        KafkaConnectCluster connect = KafkaConnectCluster.fromConfigMap(clusterCm);
        clusterCm.getData().put(KafkaConnectCluster.KEY_REPLICAS, newReplicas); // Change replicas to create ScaleDown

        when(mockCmOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockServiceOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateDeployment());

        ArgumentCaptor<String> serviceNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.patch(serviceNamespaceCaptor.capture(), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.patch(dcNamespaceCaptor.capture(), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleUpNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(dcScaleUpNamespaceCaptor.capture(), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(dcScaleDownNamespaceCaptor.capture(), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectClusterOperations ops = new KafkaConnectClusterOperations(vertx, true,
                mockCmOps, mockDcOps, mockServiceOps);

        Async async = context.async();
        ops.update(clusterCmNamespace, clusterCmName, createResult -> {
            context.assertTrue(createResult.succeeded());

            // Vertify service
            List<Service> capturedServices = serviceCaptor.getAllValues();
            context.assertEquals(0, capturedServices.size());

            // Verify Deployment
            List<Deployment> capturedDc = dcCaptor.getAllValues();
            context.assertEquals(0, capturedDc.size());

            // Verify ScaleUp
            context.assertEquals(0, dcScaleUpNameCaptor.getAllValues().size());

            // Verify scaleDown
            context.assertEquals(1, dcScaleDownNameCaptor.getAllValues().size());
            context.assertEquals(clusterCmNamespace, dcScaleDownNamespaceCaptor.getValue());
            context.assertEquals(connect.getName(), dcScaleDownNameCaptor.getValue());
            context.assertEquals(newReplicas, dcScaleDownReplicasCaptor.getValue().toString());

            async.complete();
        });
    }

    @Test
    public void testDeleteCluster(TestContext context) {
        ConfigMapOperations mockCmOps = mock(ConfigMapOperations.class);
        ServiceOperations mockServiceOps = mock(ServiceOperations.class);
        DeploymentOperations mockDcOps = mock(DeploymentOperations.class);

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaConnectCluster connect = KafkaConnectCluster.fromConfigMap(ResourceUtils.createEmptyKafkaConnectClusterConfigMap(clusterCmNamespace, clusterCmName));

        when(mockDcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateDeployment());

        ArgumentCaptor<String> serviceNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        when(mockServiceOps.delete(serviceNamespaceCaptor.capture(), serviceNameCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        when(mockDcOps.delete(dcNamespaceCaptor.capture(), dcNameCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectClusterOperations ops = new KafkaConnectClusterOperations(vertx, true,
                mockCmOps, mockDcOps, mockServiceOps);

        Async async = context.async();
        ops.delete(clusterCmNamespace, clusterCmName, createResult -> {
            context.assertTrue(createResult.succeeded());

            // Vertify service
            context.assertEquals(1, serviceNameCaptor.getAllValues().size());
            context.assertEquals(clusterCmNamespace, serviceNamespaceCaptor.getValue());
            context.assertEquals(connect.getName(), serviceNameCaptor.getValue());

            // Vertify Deployment
            context.assertEquals(1, dcNameCaptor.getAllValues().size());
            context.assertEquals(clusterCmNamespace, dcNamespaceCaptor.getValue());
            context.assertEquals(connect.getName(), dcNameCaptor.getValue());

            async.complete();
        });
    }

    @Test
    public void testReconcile(TestContext context) {
        ConfigMapOperations mockCmOps = mock(ConfigMapOperations.class);
        ServiceOperations mockServiceOps = mock(ServiceOperations.class);
        DeploymentOperations mockDcOps = mock(DeploymentOperations.class);


        String clusterCmNamespace = "test";

        ConfigMap foo = ResourceUtils.createEmptyKafkaConnectClusterConfigMap(clusterCmNamespace, "foo");
        ConfigMap bar = ResourceUtils.createEmptyKafkaConnectClusterConfigMap(clusterCmNamespace, "bar");
        ConfigMap baz = ResourceUtils.createEmptyKafkaConnectClusterConfigMap(clusterCmNamespace, "baz");
        when(mockCmOps.list(eq(clusterCmNamespace), any())).thenReturn(asList(foo, bar));
        // when requested ConfigMap for a specific Kafka Connect cluster
        when(mockCmOps.get(eq(clusterCmNamespace), eq("foo"))).thenReturn(foo);
        when(mockCmOps.get(eq(clusterCmNamespace), eq("bar"))).thenReturn(bar);

        // providing the list of ALL Deployments for all the Kafka Connect clusters
        Labels newLabels = Labels.forType("kafka-connect");
        when(mockDcOps.list(eq(clusterCmNamespace), eq(newLabels))).thenReturn(
                asList(KafkaConnectCluster.fromConfigMap(bar).generateDeployment(),
                        KafkaConnectCluster.fromConfigMap(baz).generateDeployment()));

        // providing the list Deployments for already "existing" Kafka Connect clusters
        Labels barLabels = Labels.forCluster("bar");
        when(mockDcOps.list(eq(clusterCmNamespace), eq(barLabels))).thenReturn(
                asList(KafkaConnectCluster.fromConfigMap(bar).generateDeployment())
        );

        Labels bazLabels = Labels.forCluster("baz");
        when(mockDcOps.list(eq(clusterCmNamespace), eq(bazLabels))).thenReturn(
                asList(KafkaConnectCluster.fromConfigMap(baz).generateDeployment())
        );


        Set<String> created = new HashSet<>();
        Set<String> updated = new HashSet<>();
        Set<String> deleted = new HashSet<>();

        Async async = context.async(3);
        KafkaConnectClusterOperations ops = new KafkaConnectClusterOperations(vertx, true,
                mockCmOps, mockDcOps, mockServiceOps) {

            @Override
            public void create(String namespace, String name, Handler h) {
                created.add(name);
                async.countDown();
                h.handle(Future.succeededFuture());
            }
            @Override
            public void update(String namespace, String name, Handler h) {
                updated.add(name);
                async.countDown();
                h.handle(Future.succeededFuture());
            }
            @Override
            public void delete(String namespace, String name, Handler h) {
                deleted.add(name);
                async.countDown();
                h.handle(Future.succeededFuture());
            }
        };

        // Now try to reconcile all the Kafka Connect clusters
        ops.reconcileAll(clusterCmNamespace, Labels.EMPTY);

        async.await();

        context.assertEquals(singleton("foo"), created);
        context.assertEquals(singleton("bar"), updated);
        context.assertEquals(singleton("baz"), deleted);
    }

}
