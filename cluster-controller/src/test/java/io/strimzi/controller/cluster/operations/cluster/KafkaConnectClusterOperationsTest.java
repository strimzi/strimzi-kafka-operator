/*
 * Copyright 2018 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import java.util.List;

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

        KafkaConnectCluster connect = KafkaConnectCluster.fromConfigMap(true, clusterCm);

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
        KafkaConnectCluster connect = KafkaConnectCluster.fromConfigMap(true, clusterCm);
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
        KafkaConnectCluster connect = KafkaConnectCluster.fromConfigMap(true, clusterCm);
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

            KafkaConnectCluster compareTo = KafkaConnectCluster.fromConfigMap(true, clusterCm);

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
        KafkaConnectCluster connect = KafkaConnectCluster.fromConfigMap(true, clusterCm);
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
        KafkaConnectCluster connect = KafkaConnectCluster.fromConfigMap(true, clusterCm);
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
        KafkaConnectCluster connect = KafkaConnectCluster.fromConfigMap(true, clusterCm);
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

        KafkaConnectCluster connect = KafkaConnectCluster.fromConfigMap(true, ResourceUtils.createEmptyKafkaConnectClusterConfigMap(clusterCmNamespace, clusterCmName));

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

}
