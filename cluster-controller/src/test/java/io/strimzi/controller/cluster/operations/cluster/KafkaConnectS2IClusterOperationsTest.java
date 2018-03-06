/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.openshift.api.model.BuildConfig;
import io.fabric8.openshift.api.model.DeploymentConfig;
import io.fabric8.openshift.api.model.ImageStream;
import io.strimzi.controller.cluster.ResourceUtils;
import io.strimzi.controller.cluster.operations.resource.BuildConfigOperations;
import io.strimzi.controller.cluster.operations.resource.ConfigMapOperations;
import io.strimzi.controller.cluster.operations.resource.DeploymentConfigOperations;
import io.strimzi.controller.cluster.operations.resource.ImageStreamOperations;
import io.strimzi.controller.cluster.operations.resource.ServiceOperations;
import io.strimzi.controller.cluster.resources.KafkaConnectCluster;
import io.strimzi.controller.cluster.resources.KafkaConnectS2ICluster;
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
public class KafkaConnectS2IClusterOperationsTest {

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
        DeploymentConfigOperations mockDcOps = mock(DeploymentConfigOperations.class);
        BuildConfigOperations mockBcOps = mock(BuildConfigOperations.class);
        ImageStreamOperations mockIsOps = mock(ImageStreamOperations.class);

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        ConfigMap clusterCm = ResourceUtils.createEmptyKafkaConnectS2IClusterConfigMap(clusterCmNamespace, clusterCmName);
        when(mockCmOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);

        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.create(serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<DeploymentConfig> dcCaptor = ArgumentCaptor.forClass(DeploymentConfig.class);
        when(mockDcOps.create(dcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<ImageStream> isCaptor = ArgumentCaptor.forClass(ImageStream.class);
        when(mockIsOps.create(isCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<BuildConfig> bcCaptor = ArgumentCaptor.forClass(BuildConfig.class);
        when(mockBcOps.create(bcCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectS2IClusterOperations ops = new KafkaConnectS2IClusterOperations(vertx, true,
                mockCmOps, mockDcOps, mockServiceOps, mockIsOps, mockBcOps);

        KafkaConnectS2ICluster connect = KafkaConnectS2ICluster.fromConfigMap(clusterCm);

        Async async = context.async();
        ops.create(clusterCmNamespace, clusterCmName, createResult -> {
            context.assertTrue(createResult.succeeded());

            // Vertify service
            List<Service> capturedServices = serviceCaptor.getAllValues();
            context.assertEquals(1, capturedServices.size());
            Service service = capturedServices.get(0);
            context.assertEquals(connect.getName(), service.getMetadata().getName());
            context.assertEquals(connect.generateService(), service, "Services are not equal");

            // Verify Deployment Config
            List<DeploymentConfig> capturedDc = dcCaptor.getAllValues();
            context.assertEquals(1, capturedDc.size());
            DeploymentConfig dc = capturedDc.get(0);
            context.assertEquals(connect.getName(), dc.getMetadata().getName());
            context.assertEquals(connect.generateDeploymentConfig(), dc, "Deployment Configs are not equal");

            // Verify Build Config
            List<BuildConfig> capturedBc = bcCaptor.getAllValues();
            context.assertEquals(1, capturedBc.size());
            BuildConfig bc = capturedBc.get(0);
            context.assertEquals(connect.getName(), dc.getMetadata().getName());
            context.assertEquals(connect.generateBuildConfig(), bc, "Build Configs are not equal");

            // Verify Image Streams
            List<ImageStream> capturedIs = isCaptor.getAllValues();
            context.assertEquals(2, capturedIs.size());
            int sisIndex = (connect.getSourceImageStreamName()).equals(capturedIs.get(0).getMetadata().getName()) ? 0 : 1;
            int tisIndex = (connect.getName()).equals(capturedIs.get(0).getMetadata().getName()) ? 0 : 1;

            ImageStream sis = capturedIs.get(sisIndex);
            context.assertEquals(connect.getSourceImageStreamName(), sis.getMetadata().getName());
            context.assertEquals(connect.generateSourceImageStream(), sis, "Source Image Streams are not equal");

            ImageStream tis = capturedIs.get(tisIndex);
            context.assertEquals(connect.getName(), tis.getMetadata().getName());
            context.assertEquals(connect.generateTargetImageStream(), tis, "Target Image Streams are not equal");

            async.complete();
        });
    }

    @Test
    public void testUpdateClusterNoDiff(TestContext context) {
        ConfigMapOperations mockCmOps = mock(ConfigMapOperations.class);
        ServiceOperations mockServiceOps = mock(ServiceOperations.class);
        DeploymentConfigOperations mockDcOps = mock(DeploymentConfigOperations.class);
        BuildConfigOperations mockBcOps = mock(BuildConfigOperations.class);
        ImageStreamOperations mockIsOps = mock(ImageStreamOperations.class);

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        ConfigMap clusterCm = ResourceUtils.createEmptyKafkaConnectS2IClusterConfigMap(clusterCmNamespace, clusterCmName);
        KafkaConnectS2ICluster connect = KafkaConnectS2ICluster.fromConfigMap(clusterCm);
        when(mockCmOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockServiceOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateDeploymentConfig());
        when(mockIsOps.get(clusterCmNamespace, connect.getSourceImageStreamName())).thenReturn(connect.generateSourceImageStream());
        when(mockIsOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateTargetImageStream());
        when(mockBcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateBuildConfig());

        ArgumentCaptor<String> serviceNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.patch(serviceNamespaceCaptor.capture(), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<DeploymentConfig> dcCaptor = ArgumentCaptor.forClass(DeploymentConfig.class);
        when(mockDcOps.patch(dcNamespaceCaptor.capture(), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleUpNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(dcScaleUpNamespaceCaptor.capture(), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(dcScaleDownNamespaceCaptor.capture(), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> isNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> isNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ImageStream> isCaptor = ArgumentCaptor.forClass(ImageStream.class);
        when(mockIsOps.patch(isNamespaceCaptor.capture(), isNameCaptor.capture(), isCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> bcNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> bcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<BuildConfig> bcCaptor = ArgumentCaptor.forClass(BuildConfig.class);
        when(mockBcOps.patch(bcNamespaceCaptor.capture(), bcNameCaptor.capture(), bcCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectS2IClusterOperations ops = new KafkaConnectS2IClusterOperations(vertx, true,
                mockCmOps, mockDcOps, mockServiceOps, mockIsOps, mockBcOps);

        Async async = context.async();
        ops.update(clusterCmNamespace, clusterCmName, createResult -> {
            context.assertTrue(createResult.succeeded());

            // Vertify service
            List<Service> capturedServices = serviceCaptor.getAllValues();
            context.assertEquals(0, capturedServices.size());

            // Verify Deployment Config
            List<DeploymentConfig> capturedDc = dcCaptor.getAllValues();
            context.assertEquals(0, capturedDc.size());

            // Verify Build Config
            List<BuildConfig> capturedBc = bcCaptor.getAllValues();
            context.assertEquals(0, capturedBc.size());

            // Verify Image Streams
            List<ImageStream> capturedIs = isCaptor.getAllValues();
            context.assertEquals(0, capturedIs.size());

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
        DeploymentConfigOperations mockDcOps = mock(DeploymentConfigOperations.class);
        BuildConfigOperations mockBcOps = mock(BuildConfigOperations.class);
        ImageStreamOperations mockIsOps = mock(ImageStreamOperations.class);

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        ConfigMap clusterCm = ResourceUtils.createEmptyKafkaConnectS2IClusterConfigMap(clusterCmNamespace, clusterCmName);
        KafkaConnectS2ICluster connect = KafkaConnectS2ICluster.fromConfigMap(clusterCm);
        clusterCm.getData().put("image", "some/different:image"); // Change the image to generate some diff

        when(mockCmOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockServiceOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateDeploymentConfig());
        when(mockIsOps.get(clusterCmNamespace, connect.getSourceImageStreamName())).thenReturn(connect.generateSourceImageStream());
        when(mockIsOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateTargetImageStream());
        when(mockBcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateBuildConfig());

        ArgumentCaptor<String> serviceNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.patch(serviceNamespaceCaptor.capture(), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<DeploymentConfig> dcCaptor = ArgumentCaptor.forClass(DeploymentConfig.class);
        when(mockDcOps.patch(dcNamespaceCaptor.capture(), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleUpNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(dcScaleUpNamespaceCaptor.capture(), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(dcScaleDownNamespaceCaptor.capture(), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> isNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> isNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ImageStream> isCaptor = ArgumentCaptor.forClass(ImageStream.class);
        when(mockIsOps.patch(isNamespaceCaptor.capture(), isNameCaptor.capture(), isCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> bcNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> bcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<BuildConfig> bcCaptor = ArgumentCaptor.forClass(BuildConfig.class);
        when(mockBcOps.patch(bcNamespaceCaptor.capture(), bcNameCaptor.capture(), bcCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectS2IClusterOperations ops = new KafkaConnectS2IClusterOperations(vertx, true,
                mockCmOps, mockDcOps, mockServiceOps, mockIsOps, mockBcOps);

        Async async = context.async();
        ops.update(clusterCmNamespace, clusterCmName, createResult -> {
            context.assertTrue(createResult.succeeded());

            KafkaConnectS2ICluster compareTo = KafkaConnectS2ICluster.fromConfigMap(clusterCm);

            // Vertify service
            List<Service> capturedServices = serviceCaptor.getAllValues();
            context.assertEquals(1, capturedServices.size());
            Service service = capturedServices.get(0);
            context.assertEquals(compareTo.getName(), service.getMetadata().getName());
            context.assertEquals(compareTo.generateService(), service, "Services are not equal");

            // Verify Deployment Config
            List<DeploymentConfig> capturedDc = dcCaptor.getAllValues();
            context.assertEquals(1, capturedDc.size());
            DeploymentConfig dc = capturedDc.get(0);
            context.assertEquals(compareTo.getName(), dc.getMetadata().getName());
            context.assertEquals(compareTo.generateDeploymentConfig(), dc, "Deployment Configs are not equal");

            // Verify Build Config
            List<BuildConfig> capturedBc = bcCaptor.getAllValues();
            context.assertEquals(1, capturedBc.size());
            BuildConfig bc = capturedBc.get(0);
            context.assertEquals(compareTo.getName(), dc.getMetadata().getName());
            context.assertEquals(compareTo.generateBuildConfig(), bc, "Build Configs are not equal");

            // Verify Image Streams
            List<ImageStream> capturedIs = isCaptor.getAllValues();
            context.assertEquals(2, capturedIs.size());
            int sisIndex = (compareTo.getSourceImageStreamName()).equals(capturedIs.get(0).getMetadata().getName()) ? 0 : 1;
            int tisIndex = (compareTo.getName()).equals(capturedIs.get(0).getMetadata().getName()) ? 0 : 1;

            ImageStream sis = capturedIs.get(sisIndex);
            context.assertEquals(compareTo.getSourceImageStreamName(), sis.getMetadata().getName());
            context.assertEquals(compareTo.generateSourceImageStream(), sis, "Source Image Streams are not equal");

            ImageStream tis = capturedIs.get(tisIndex);
            context.assertEquals(compareTo.getName(), tis.getMetadata().getName());
            context.assertEquals(compareTo.generateTargetImageStream(), tis, "Target Image Streams are not equal");

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
        DeploymentConfigOperations mockDcOps = mock(DeploymentConfigOperations.class);
        BuildConfigOperations mockBcOps = mock(BuildConfigOperations.class);
        ImageStreamOperations mockIsOps = mock(ImageStreamOperations.class);

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        ConfigMap clusterCm = ResourceUtils.createEmptyKafkaConnectS2IClusterConfigMap(clusterCmNamespace, clusterCmName);
        KafkaConnectS2ICluster connect = KafkaConnectS2ICluster.fromConfigMap(clusterCm);
        clusterCm.getData().put("image", "some/different:image"); // Change the image to generate some diff

        when(mockCmOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockServiceOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateDeploymentConfig());
        when(mockIsOps.get(clusterCmNamespace, connect.getSourceImageStreamName())).thenReturn(connect.generateSourceImageStream());
        when(mockIsOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateTargetImageStream());
        when(mockBcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateBuildConfig());

        ArgumentCaptor<String> serviceNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.patch(serviceNamespaceCaptor.capture(), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<DeploymentConfig> dcCaptor = ArgumentCaptor.forClass(DeploymentConfig.class);
        when(mockDcOps.patch(dcNamespaceCaptor.capture(), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.failedFuture("Failed"));

        ArgumentCaptor<String> dcScaleUpNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(dcScaleUpNamespaceCaptor.capture(), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(dcScaleDownNamespaceCaptor.capture(), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> isNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> isNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ImageStream> isCaptor = ArgumentCaptor.forClass(ImageStream.class);
        when(mockIsOps.patch(isNamespaceCaptor.capture(), isNameCaptor.capture(), isCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> bcNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> bcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<BuildConfig> bcCaptor = ArgumentCaptor.forClass(BuildConfig.class);
        when(mockBcOps.patch(bcNamespaceCaptor.capture(), bcNameCaptor.capture(), bcCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectS2IClusterOperations ops = new KafkaConnectS2IClusterOperations(vertx, true,
                mockCmOps, mockDcOps, mockServiceOps, mockIsOps, mockBcOps);

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
        DeploymentConfigOperations mockDcOps = mock(DeploymentConfigOperations.class);
        BuildConfigOperations mockBcOps = mock(BuildConfigOperations.class);
        ImageStreamOperations mockIsOps = mock(ImageStreamOperations.class);

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        ConfigMap clusterCm = ResourceUtils.createEmptyKafkaConnectS2IClusterConfigMap(clusterCmNamespace, clusterCmName);
        KafkaConnectS2ICluster connect = KafkaConnectS2ICluster.fromConfigMap(clusterCm);
        clusterCm.getData().put(KafkaConnectCluster.KEY_REPLICAS, newReplicas); // Change replicas to create ScaleUp

        when(mockCmOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockServiceOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateDeploymentConfig());
        when(mockIsOps.get(clusterCmNamespace, connect.getSourceImageStreamName())).thenReturn(connect.generateSourceImageStream());
        when(mockIsOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateTargetImageStream());
        when(mockBcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateBuildConfig());

        ArgumentCaptor<String> serviceNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.patch(serviceNamespaceCaptor.capture(), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<DeploymentConfig> dcCaptor = ArgumentCaptor.forClass(DeploymentConfig.class);
        when(mockDcOps.patch(dcNamespaceCaptor.capture(), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleUpNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(dcScaleUpNamespaceCaptor.capture(), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(dcScaleDownNamespaceCaptor.capture(), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> isNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> isNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ImageStream> isCaptor = ArgumentCaptor.forClass(ImageStream.class);
        when(mockIsOps.patch(isNamespaceCaptor.capture(), isNameCaptor.capture(), isCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> bcNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> bcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<BuildConfig> bcCaptor = ArgumentCaptor.forClass(BuildConfig.class);
        when(mockBcOps.patch(bcNamespaceCaptor.capture(), bcNameCaptor.capture(), bcCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectS2IClusterOperations ops = new KafkaConnectS2IClusterOperations(vertx, true,
                mockCmOps, mockDcOps, mockServiceOps, mockIsOps, mockBcOps);

        Async async = context.async();
        ops.update(clusterCmNamespace, clusterCmName, createResult -> {
            context.assertTrue(createResult.succeeded());

            // Vertify service
            List<Service> capturedServices = serviceCaptor.getAllValues();
            context.assertEquals(0, capturedServices.size());

            // Verify Deployment Config
            List<DeploymentConfig> capturedDc = dcCaptor.getAllValues();
            context.assertEquals(0, capturedDc.size());

            // Verify Build Config
            List<BuildConfig> capturedBc = bcCaptor.getAllValues();
            context.assertEquals(0, capturedBc.size());

            // Verify Image Streams
            List<ImageStream> capturedIs = isCaptor.getAllValues();
            context.assertEquals(0, capturedIs.size());

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
        DeploymentConfigOperations mockDcOps = mock(DeploymentConfigOperations.class);
        BuildConfigOperations mockBcOps = mock(BuildConfigOperations.class);
        ImageStreamOperations mockIsOps = mock(ImageStreamOperations.class);

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        ConfigMap clusterCm = ResourceUtils.createEmptyKafkaConnectS2IClusterConfigMap(clusterCmNamespace, clusterCmName);
        KafkaConnectS2ICluster connect = KafkaConnectS2ICluster.fromConfigMap(clusterCm);
        clusterCm.getData().put(KafkaConnectCluster.KEY_REPLICAS, newReplicas); // Change replicas to create ScaleDown

        when(mockCmOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockServiceOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateDeploymentConfig());
        when(mockIsOps.get(clusterCmNamespace, connect.getSourceImageStreamName())).thenReturn(connect.generateSourceImageStream());
        when(mockIsOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateTargetImageStream());
        when(mockBcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateBuildConfig());

        ArgumentCaptor<String> serviceNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.patch(serviceNamespaceCaptor.capture(), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<DeploymentConfig> dcCaptor = ArgumentCaptor.forClass(DeploymentConfig.class);
        when(mockDcOps.patch(dcNamespaceCaptor.capture(), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleUpNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(dcScaleUpNamespaceCaptor.capture(), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(dcScaleDownNamespaceCaptor.capture(), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> isNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> isNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ImageStream> isCaptor = ArgumentCaptor.forClass(ImageStream.class);
        when(mockIsOps.patch(isNamespaceCaptor.capture(), isNameCaptor.capture(), isCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> bcNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> bcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<BuildConfig> bcCaptor = ArgumentCaptor.forClass(BuildConfig.class);
        when(mockBcOps.patch(bcNamespaceCaptor.capture(), bcNameCaptor.capture(), bcCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectS2IClusterOperations ops = new KafkaConnectS2IClusterOperations(vertx, true,
                mockCmOps, mockDcOps, mockServiceOps, mockIsOps, mockBcOps);

        Async async = context.async();
        ops.update(clusterCmNamespace, clusterCmName, createResult -> {
            context.assertTrue(createResult.succeeded());

            // Vertify service
            List<Service> capturedServices = serviceCaptor.getAllValues();
            context.assertEquals(0, capturedServices.size());

            // Verify Deployment Config
            List<DeploymentConfig> capturedDc = dcCaptor.getAllValues();
            context.assertEquals(0, capturedDc.size());

            // Verify Build Config
            List<BuildConfig> capturedBc = bcCaptor.getAllValues();
            context.assertEquals(0, capturedBc.size());

            // Verify Image Streams
            List<ImageStream> capturedIs = isCaptor.getAllValues();
            context.assertEquals(0, capturedIs.size());

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
        DeploymentConfigOperations mockDcOps = mock(DeploymentConfigOperations.class);
        BuildConfigOperations mockBcOps = mock(BuildConfigOperations.class);
        ImageStreamOperations mockIsOps = mock(ImageStreamOperations.class);

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaConnectS2ICluster connect = KafkaConnectS2ICluster.fromConfigMap(ResourceUtils.createEmptyKafkaConnectS2IClusterConfigMap(clusterCmNamespace, clusterCmName));

        when(mockDcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateDeploymentConfig());
        when(mockIsOps.get(clusterCmNamespace, connect.getSourceImageStreamName())).thenReturn(connect.generateSourceImageStream());

        ArgumentCaptor<String> serviceNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        when(mockServiceOps.delete(serviceNamespaceCaptor.capture(), serviceNameCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        when(mockDcOps.delete(dcNamespaceCaptor.capture(), dcNameCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> isNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> isNameCaptor = ArgumentCaptor.forClass(String.class);
        when(mockIsOps.delete(isNamespaceCaptor.capture(), isNameCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> bcNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> bcNameCaptor = ArgumentCaptor.forClass(String.class);
        when(mockBcOps.delete(bcNamespaceCaptor.capture(), bcNameCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectS2IClusterOperations ops = new KafkaConnectS2IClusterOperations(vertx, true,
                mockCmOps, mockDcOps, mockServiceOps, mockIsOps, mockBcOps);

        Async async = context.async();
        ops.delete(clusterCmNamespace, clusterCmName, createResult -> {
            context.assertTrue(createResult.succeeded());

            // Vertify service
            context.assertEquals(1, serviceNameCaptor.getAllValues().size());
            context.assertEquals(clusterCmNamespace, serviceNamespaceCaptor.getValue());
            context.assertEquals(connect.getName(), serviceNameCaptor.getValue());

            // Vertify deployment Config
            context.assertEquals(1, dcNameCaptor.getAllValues().size());
            context.assertEquals(clusterCmNamespace, dcNamespaceCaptor.getValue());
            context.assertEquals(connect.getName(), dcNameCaptor.getValue());

            // Vertify BuildConfig
            context.assertEquals(1, bcNameCaptor.getAllValues().size());
            context.assertEquals(clusterCmNamespace, bcNamespaceCaptor.getValue());
            context.assertEquals(connect.getName(), bcNameCaptor.getValue());

            // Vertify ImageStreams
            int sisIndex = (connect.getSourceImageStreamName()).equals(isNameCaptor.getAllValues().get(0)) ? 0 : 1;
            int tisIndex = (connect.getName()).equals(isNameCaptor.getAllValues().get(0)) ? 0 : 1;
            context.assertEquals(2, isNameCaptor.getAllValues().size());
            context.assertEquals(clusterCmNamespace, isNamespaceCaptor.getAllValues().get(sisIndex));
            context.assertEquals(connect.getSourceImageStreamName(), isNameCaptor.getAllValues().get(sisIndex));
            context.assertEquals(clusterCmNamespace, isNamespaceCaptor.getAllValues().get(tisIndex));
            context.assertEquals(connect.getName(), isNameCaptor.getAllValues().get(tisIndex));


            async.complete();
        });
    }

    @Test
    public void testReconcile(TestContext context) {
        ConfigMapOperations mockCmOps = mock(ConfigMapOperations.class);
        ServiceOperations mockServiceOps = mock(ServiceOperations.class);
        DeploymentConfigOperations mockDcOps = mock(DeploymentConfigOperations.class);
        BuildConfigOperations mockBcOps = mock(BuildConfigOperations.class);
        ImageStreamOperations mockIsOps = mock(ImageStreamOperations.class);


        String clusterCmNamespace = "test";

        ConfigMap foo = ResourceUtils.createEmptyKafkaConnectClusterConfigMap(clusterCmNamespace, "foo");
        ConfigMap bar = ResourceUtils.createEmptyKafkaConnectClusterConfigMap(clusterCmNamespace, "bar");
        ConfigMap baz = ResourceUtils.createEmptyKafkaConnectClusterConfigMap(clusterCmNamespace, "baz");
        when(mockCmOps.list(eq(clusterCmNamespace), any())).thenReturn(asList(foo, bar));
        // when requested ConfigMap for a specific Kafka Connect S2I cluster
        when(mockCmOps.get(eq(clusterCmNamespace), eq("foo"))).thenReturn(foo);
        when(mockCmOps.get(eq(clusterCmNamespace), eq("bar"))).thenReturn(bar);

        // providing the list of ALL DeploymentConfigs for all the Kafka Connect S2I clusters
        Labels newLabels = Labels.type("kafka-connect-s2i");
        when(mockDcOps.list(eq(clusterCmNamespace), eq(newLabels))).thenReturn(
                asList(KafkaConnectS2ICluster.fromConfigMap(bar).generateDeploymentConfig(),
                        KafkaConnectS2ICluster.fromConfigMap(baz).generateDeploymentConfig()));

        // providing the list DeploymentConfigs for already "existing" Kafka Connect S2I clusters
        Labels barLabels = Labels.cluster("bar");
        when(mockDcOps.list(eq(clusterCmNamespace), eq(barLabels))).thenReturn(
                asList(KafkaConnectS2ICluster.fromConfigMap(bar).generateDeploymentConfig())
        );

        Labels bazLabels = Labels.cluster("baz");
        when(mockDcOps.list(eq(clusterCmNamespace), eq(bazLabels))).thenReturn(
                asList(KafkaConnectS2ICluster.fromConfigMap(baz).generateDeploymentConfig())
        );


        Set<String> created = new HashSet<>();
        Set<String> updated = new HashSet<>();
        Set<String> deleted = new HashSet<>();

        Async async = context.async(3);
        KafkaConnectS2IClusterOperations ops = new KafkaConnectS2IClusterOperations(vertx, true,
                mockCmOps, mockDcOps, mockServiceOps, mockIsOps, mockBcOps) {

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

        // Now try to reconcile all the Kafka Connect S2I clusters
        ops.reconcileAll(clusterCmNamespace, Labels.EMPTY);

        async.await();

        context.assertEquals(singleton("foo"), created);
        context.assertEquals(singleton("bar"), updated);
        context.assertEquals(singleton("baz"), deleted);
    }

}
