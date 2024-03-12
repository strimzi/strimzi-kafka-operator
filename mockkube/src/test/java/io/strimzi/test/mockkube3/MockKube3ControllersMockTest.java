/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.mockkube3;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.test.TestUtils;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class MockKube3ControllersMockTest {
    private static KubernetesClient client;
    private static MockKube3 mockKube;

    private String namespace;

    @BeforeAll
    public static void beforeAll() {
        // Configure the Kubernetes Mock
        mockKube = new MockKube3.MockKube3Builder()
                .withDeploymentController()
                .withPodController()
                .withServiceController()
                .withDeletionController()
                .build();
        mockKube.start();
        client = mockKube.client();
    }

    @AfterAll
    public static void afterAll() {
        mockKube.stop();
    }

    @BeforeEach
    public void beforeEach(TestInfo testInfo)   {
        namespace = testInfo.getTestMethod().orElseThrow().getName().toLowerCase(Locale.ROOT);
        mockKube.prepareNamespace(namespace);
    }

    @AfterEach
    public void afterEach(TestInfo testInfo)    {
        client.namespaces().withName(testInfo.getTestMethod().orElseThrow().getName().toLowerCase(Locale.ROOT)).delete();
    }

    @Test
    public void testServiceController() {
        final String serviceName = "my-service";

        Service svc = new ServiceBuilder()
                .withNewMetadata()
                    .withNamespace(namespace)
                    .withName(serviceName)
                .endMetadata()
                .withNewSpec()
                    .withSelector(Map.of("app", "my-app"))
                    .withPorts(new ServicePortBuilder().withProtocol("TCP").withPort(80).withTargetPort(new IntOrString(8080)).build())
                .endSpec()
                .build();

        client.services().inNamespace(namespace).resource(svc).create();

        TestUtils.waitFor("Wait for service to be created", 100L, 10_000L, () -> client.services().inNamespace(namespace).withName(serviceName).get() != null);

        Service createdSvc = client.services().inNamespace(namespace).withName(serviceName).get();
        assertThat(createdSvc, is(notNullValue()));

        TestUtils.waitFor("Wait for endpoints to be created", 100L, 10_000L, () -> client.endpoints().inNamespace(namespace).withName(serviceName).get() != null);

        Endpoints createdEndpoints = client.endpoints().inNamespace(namespace).withName(serviceName).get();
        assertThat(createdEndpoints, is(notNullValue()));
        assertThat(createdEndpoints.getSubsets(), is(notNullValue()));
        assertThat(createdEndpoints.getSubsets().size(), is(1));

        client.services().inNamespace(namespace).withName(serviceName).withPropagationPolicy(DeletionPropagation.BACKGROUND).delete();
        client.services().inNamespace(namespace).withName(serviceName).waitUntilCondition(Objects::isNull, 10_000, TimeUnit.MILLISECONDS);
        assertThat(client.services().inNamespace(namespace).withName(serviceName).get(), CoreMatchers.is(nullValue()));
    }

    @Test
    public void testDeploymentController() {
        final String deploymentName = "my-deployment";

        Deployment dep = new DeploymentBuilder()
                .withNewMetadata()
                    .withNamespace(namespace)
                    .withName(deploymentName)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(3)
                    .withSelector(new LabelSelector(null, Map.of("app", "my-app")))
                    .withNewTemplate()
                        .withNewMetadata()
                            .withLabels(Map.of("app", "my-app"))
                        .endMetadata()
                        .withNewSpec()
                            .withContainers(new ContainerBuilder().withName("nginx").withImage("nginx:1.14.2").build())
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();

        client.apps().deployments().inNamespace(namespace).resource(dep).create();

        TestUtils.waitFor("Wait for deployment to have status", 100L, 10_000L, () -> client.apps().deployments().inNamespace(namespace).withName(deploymentName).get() != null && client.apps().deployments().inNamespace(namespace).withName(deploymentName).get().getStatus() != null);

        Deployment createdDeployment = client.apps().deployments().inNamespace(namespace).withName(deploymentName).get();
        assertThat(createdDeployment, is(notNullValue()));
        assertThat(createdDeployment.getStatus(), is(notNullValue()));
        assertThat(createdDeployment.getStatus().getReplicas(), is(3));
        assertThat(createdDeployment.getStatus().getAvailableReplicas(), is(3));
        assertThat(createdDeployment.getStatus().getObservedGeneration(), is(1L));

        client.apps().deployments().inNamespace(namespace).withName(deploymentName).withPropagationPolicy(DeletionPropagation.BACKGROUND).delete();
        client.apps().deployments().inNamespace(namespace).withName(deploymentName).waitUntilCondition(Objects::isNull, 10_000, TimeUnit.MILLISECONDS);
        assertThat(client.apps().deployments().inNamespace(namespace).withName(deploymentName).get(), CoreMatchers.is(nullValue()));
    }

    @Test
    public void testPodController() {
        final String podName = "my-pod";

        Pod pod = new PodBuilder()
                .withNewMetadata()
                    .withNamespace(namespace)
                    .withName(podName)
                .endMetadata()
                .withNewSpec()
                    .withContainers(new ContainerBuilder().withName("nginx").withImage("nginx:1.14.2").build())
                .endSpec()
                .build();

        client.pods().inNamespace(namespace).resource(pod).create();

        TestUtils.waitFor("Wait for pod to have status", 100L, 10_000L,
                () -> client.pods().inNamespace(namespace).withName(podName).get() != null
                        && client.pods().inNamespace(namespace).withName(podName).get().getStatus() != null
                        && client.pods().inNamespace(namespace).withName(podName).get().getStatus().getConditions() != null
                        && !client.pods().inNamespace(namespace).withName(podName).get().getStatus().getConditions().isEmpty());

        Pod createdPod = client.pods().inNamespace(namespace).withName(podName).get();
        assertThat(createdPod, is(notNullValue()));
        assertThat(createdPod.getStatus(), is(notNullValue()));
        assertThat(createdPod.getStatus().getConditions(), is(notNullValue()));
        assertThat(createdPod.getStatus().getConditions().size(), is(1));
        assertThat(createdPod.getStatus().getConditions().get(0).getType(), is("Ready"));
        assertThat(createdPod.getStatus().getConditions().get(0).getStatus(), is("True"));

        client.pods().inNamespace(namespace).withName(podName).withPropagationPolicy(DeletionPropagation.BACKGROUND).delete();
        client.pods().inNamespace(namespace).withName(podName).waitUntilCondition(Objects::isNull, 10_000, TimeUnit.MILLISECONDS);
        assertThat(client.pods().inNamespace(namespace).withName(podName).get(), CoreMatchers.is(nullValue()));
    }

    @Test
    public void testPVCDeletion() {
        final String pvcName = "my-pvc";

        PersistentVolumeClaim pvc = new PersistentVolumeClaimBuilder()
                .withNewMetadata()
                    .withNamespace(namespace)
                    .withName(pvcName)
                .endMetadata()
                .withNewSpec()
                    .withNewResources()
                        .withRequests(singletonMap("storage", new Quantity("100Gi")))
                    .endResources()
                    .withAccessModes("ReadWriteOnce")
                .endSpec()
                .build();

        client.persistentVolumeClaims().inNamespace(namespace).resource(pvc).create();

        PersistentVolumeClaim createdPvc = client.persistentVolumeClaims().inNamespace(namespace).withName(pvcName).get();
        assertThat(createdPvc, is(notNullValue()));

        client.persistentVolumeClaims().inNamespace(namespace).withName(pvcName).withPropagationPolicy(DeletionPropagation.BACKGROUND).delete();
        client.persistentVolumeClaims().inNamespace(namespace).withName(pvcName).waitUntilCondition(Objects::isNull, 10_000, TimeUnit.MILLISECONDS);
        assertThat(client.persistentVolumeClaims().inNamespace(namespace).withName(pvcName).get(), CoreMatchers.is(nullValue()));
    }
}
