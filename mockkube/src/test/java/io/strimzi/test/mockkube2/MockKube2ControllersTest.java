/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.mockkube2;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@EnableKubernetesMockClient(crud = true)
public class MockKube2ControllersTest {
    private final static String NAMESPACE = "my-namespace";

    // Injected by Fabric8 Mock Kubernetes Server
    @SuppressWarnings("unused")
    private KubernetesClient client;
    private MockKube2 mockKube;

    @BeforeEach
    public void beforeEach() {
        // Configure the Kubernetes Mock
        mockKube = new MockKube2.MockKube2Builder(client)
                .withDeploymentController()
                .withPodController()
                .withServiceController()
                .build();
        mockKube.start();
    }

    @AfterEach
    public void afterEach() {
        mockKube.stop();
    }

    @Test
    public void testServiceController() {
        final String serviceName = "my-service";

        Service svc = new ServiceBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(serviceName)
                .endMetadata()
                .withNewSpec()
                    .withSelector(Map.of("app", "my-app"))
                    .withPorts(new ServicePortBuilder().withProtocol("TCP").withPort(80).withTargetPort(new IntOrString(8080)).build())
                .endSpec()
                .build();

        client.services().inNamespace(NAMESPACE).resource(svc).create();

        TestUtils.waitFor("Wait for service to be created", 100L, 10_000L, () -> client.services().inNamespace(NAMESPACE).withName(serviceName).get() != null);

        Service createdSvc = client.services().inNamespace(NAMESPACE).withName(serviceName).get();
        assertThat(createdSvc, is(notNullValue()));

        TestUtils.waitFor("Wait for endpoints to be created", 100L, 10_000L, () -> client.endpoints().inNamespace(NAMESPACE).withName(serviceName).get() != null);

        Endpoints createdEndpoints = client.endpoints().inNamespace(NAMESPACE).withName(serviceName).get();
        assertThat(createdEndpoints, is(notNullValue()));
        assertThat(createdEndpoints.getSubsets(), is(notNullValue()));
        assertThat(createdEndpoints.getSubsets().size(), is(1));
    }

    @Test
    public void testDeploymentController() {
        final String deploymentName = "my-deployment";

        Deployment dep = new DeploymentBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(deploymentName)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(3)
                    .withSelector(new LabelSelector(null, Map.of("app", "my-app")))
                    .withNewTemplate()
                        .withNewSpec()
                            .withContainers(new ContainerBuilder().withName("nginx").withImage("nginx:1.14.2").build())
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();

        client.apps().deployments().inNamespace(NAMESPACE).resource(dep).create();

        TestUtils.waitFor("Wait for deployment to have status", 100L, 10_000L, () -> client.apps().deployments().inNamespace(NAMESPACE).withName(deploymentName).get() != null && client.apps().deployments().inNamespace(NAMESPACE).withName(deploymentName).get().getStatus() != null);

        Deployment createdDeployment = client.apps().deployments().inNamespace(NAMESPACE).withName(deploymentName).get();
        assertThat(createdDeployment, is(notNullValue()));
        assertThat(createdDeployment.getStatus(), is(notNullValue()));
        assertThat(createdDeployment.getStatus().getReplicas(), is(3));
        assertThat(createdDeployment.getStatus().getAvailableReplicas(), is(3));
        assertThat(createdDeployment.getStatus().getObservedGeneration(), is(1L));
    }

    @Test
    public void testPodController() {
        final String podName = "my-pod";

        Pod pod = new PodBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(podName)
                .endMetadata()
                .withNewSpec()
                    .withContainers(new ContainerBuilder().withName("nginx").withImage("nginx:1.14.2").build())
                .endSpec()
                .build();

        client.pods().inNamespace(NAMESPACE).resource(pod).create();

        TestUtils.waitFor("Wait for pod to have status", 100L, 10_000L, () -> client.pods().inNamespace(NAMESPACE).withName(podName).get() != null && client.pods().inNamespace(NAMESPACE).withName(podName).get().getStatus() != null);

        Pod createdPod = client.pods().inNamespace(NAMESPACE).withName(podName).get();
        assertThat(createdPod, is(notNullValue()));
        assertThat(createdPod.getStatus(), is(notNullValue()));
        assertThat(createdPod.getStatus().getConditions(), is(notNullValue()));
        assertThat(createdPod.getStatus().getConditions().size(), is(1));
        assertThat(createdPod.getStatus().getConditions().get(0).getType(), is("Ready"));
        assertThat(createdPod.getStatus().getConditions().get(0).getStatus(), is("True"));
    }
}
