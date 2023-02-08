/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.events;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ListOptions;
import io.fabric8.kubernetes.api.model.ListOptionsBuilder;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.api.model.ObjectReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.events.v1.Event;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.k8s.cmdClient.KubeCmdClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class KubernetesRestartEventPublisherIT {

    private static KubeClusterResource cluster;
    private static KubernetesClient kubeClient;
    private static KubeCmdClient<?> cmdClient;
    private static final String TEST_NAMESPACE = "v1-test-ns";
    private Pod pod;

    @BeforeAll
    static void beforeAll() {
        cluster = KubeClusterResource.getInstance();
        kubeClient = cluster.client().getClient();
        cmdClient = cluster.cmdClient();
        if (kubeClient.namespaces().withName(TEST_NAMESPACE).get() != null) {
            cluster.client().deleteNamespace(TEST_NAMESPACE);
            cmdClient.waitForResourceDeletion("namespace", TEST_NAMESPACE);
        }
        cmdClient.createNamespace(TEST_NAMESPACE);
        cmdClient.waitForResourceCreation("namespace", TEST_NAMESPACE);
        cluster.setNamespace(TEST_NAMESPACE);
    }

    @BeforeEach
    void setup() {
        String podName = "test-pod-" + new Random().nextInt();
        pod = buildPod(podName);
        kubeClient.pods().inNamespace(TEST_NAMESPACE).resource(pod).create();
        cmdClient.namespace(TEST_NAMESPACE).waitForResourceCreation("pod", podName);
    }

    @AfterAll
    static void afterAll() {
        cluster.client().deleteNamespace(TEST_NAMESPACE);
        cmdClient.waitForResourceDeletion("namespace", TEST_NAMESPACE);
    }

    @AfterEach
    void teardown() {
        kubeClient.pods().inNamespace(TEST_NAMESPACE).resource(pod).delete();
        cmdClient.namespace(TEST_NAMESPACE).waitForResourceDeletion("pod", pod.getMetadata().getName());
        kubeClient.events().v1().events().inNamespace(TEST_NAMESPACE).delete();
    }

    @Test
    void eventPublicationSucceeds() {
        KubernetesRestartEventPublisher publisher = new KubernetesRestartEventPublisher(kubeClient, "op") {
        };
        publisher.publishRestartEvents(pod, RestartReasons.of(RestartReason.CLUSTER_CA_CERT_KEY_REPLACED)
                .add(RestartReason.JBOD_VOLUMES_CHANGED));

        ListOptions strimziEventsOnly = new ListOptionsBuilder()
                .withFieldSelector("reportingController=" + KubernetesRestartEventPublisher.CONTROLLER)
                .build();
        List<Event> items = kubeClient.events().v1().events().inNamespace(TEST_NAMESPACE).list(strimziEventsOnly).getItems();
        assertThat(items, hasSize(2));
        assertThat(items.stream().map(Event::getReason).collect(toSet()), is(Set.of("ClusterCaCertKeyReplaced", "JbodVolumesChanged")));

        Event exemplar = items.get(0);
        assertThat(exemplar.getAction(), is(KubernetesRestartEventPublisher.ACTION));
        assertThat(exemplar.getRegarding(), is(referenceFromPod(pod)));
    }

    static ObjectReference referenceFromPod(Pod pod) {
        return new ObjectReferenceBuilder()
                .withKind("Pod")
                .withNamespace(pod.getMetadata().getNamespace())
                .withName(pod.getMetadata().getName())
                .build();
    }

    private static Pod buildPod(String podName) {
        return new PodBuilder()
                .withNewMetadata()
                .withName(podName)
                .withNamespace(TEST_NAMESPACE)
                .withAnnotations(new HashMap<>())
                .endMetadata()
                .withNewSpec()
                .withContainers(new ContainerBuilder()
                        .withName("busybox")
                        .withImage("quay.io/scholzj/busybox:latest") // Quay.io is used to avoid Docker Hub limits
                        .withCommand("sleep", "3600")
                        .withImagePullPolicy("IfNotPresent")
                        .build())
                .withRestartPolicy("Always")
                .withTerminationGracePeriodSeconds(0L)
                .endSpec()
                .build();
    }
}
