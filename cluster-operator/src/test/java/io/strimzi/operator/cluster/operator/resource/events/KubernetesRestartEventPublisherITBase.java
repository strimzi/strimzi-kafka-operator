/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.events;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.api.model.ObjectReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.VersionInfo;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.k8s.cmdClient.KubeCmdClient;

import java.util.HashMap;
import java.util.Random;
import java.util.function.BiFunction;

public class KubernetesRestartEventPublisherITBase {

    private static KubeClusterResource cluster;
    private static KubernetesClient kubeClient;
    private static KubeCmdClient<?> cmdClient;

    static KubernetesClient prepareNamespace(String testNamespace) {
        cluster = KubeClusterResource.getInstance();
        kubeClient = cluster.client().getClient();
        cmdClient = cluster.cmdClient();
        if (kubeClient.namespaces().withName(testNamespace).get() != null) {
            cluster.client().deleteNamespace(testNamespace);
            cmdClient.waitForResourceDeletion("namespace", testNamespace);
        }
        cmdClient.createNamespace(testNamespace);
        cmdClient.waitForResourceCreation("namespace", testNamespace);
        cluster.setNamespace(testNamespace);

        return kubeClient;
    }

    static Pod createPod(String testNamespace) {
        String podName = "test-pod-" + new Random().nextInt();
        Pod pod = buildPod(testNamespace, podName);
        kubeClient.pods().inNamespace(testNamespace).create(pod);
        cmdClient.namespace(testNamespace).waitForResourceCreation("pod", podName);
        return pod;
    }

    static void teardownPod(String testNamespace, Pod pod) {
        kubeClient.pods().inNamespace(testNamespace).delete(pod);
        cmdClient.namespace(testNamespace).waitForResourceDeletion("pod", pod.getMetadata().getName());
    }

    static void teardownNamespace(String testNamespace) {
        cluster.client().deleteNamespace(testNamespace);
        cmdClient.waitForResourceDeletion("namespace", testNamespace);
    }

    static ObjectReference referenceFromPod(Pod pod) {
        return new ObjectReferenceBuilder()
                .withKind("Pod")
                .withNamespace(pod.getMetadata().getNamespace())
                .withName(pod.getMetadata().getName())
                .build();
    }

    private static Pod buildPod(String namespace, String podName) {
        return new PodBuilder()
                .withNewMetadata()
                .withName(podName)
                .withNamespace(namespace)
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

    static boolean checkClusterVersionMatches(BiFunction<Integer, Integer, Boolean> majorMinorPred) {
        VersionInfo version = KubeClusterResource.getInstance().client().getClient().getKubernetesVersion();
        return majorMinorPred.apply(Integer.parseInt(version.getMajor()), Integer.parseInt(version.getMinor()));
    }
}
