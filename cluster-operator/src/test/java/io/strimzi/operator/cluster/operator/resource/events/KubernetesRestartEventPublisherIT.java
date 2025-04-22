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
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class KubernetesRestartEventPublisherIT {
    private static KubernetesClient client;
    private static final String TEST_NAMESPACE = "v1-test-ns";
    private Pod pod;

    @BeforeAll
    static void beforeAll() {
        client = new KubernetesClientBuilder().build();
        TestUtils.createNamespace(client, TEST_NAMESPACE);
    }

    @BeforeEach
    void setup() {
        String podName = "test-pod-" + new Random().nextInt();
        pod = buildPod(podName);
        client.pods().inNamespace(TEST_NAMESPACE).resource(pod).create();
        client.pods().inNamespace(TEST_NAMESPACE).resource(pod).waitUntilCondition(Objects::nonNull, 60_000, TimeUnit.MILLISECONDS);
    }

    @AfterAll
    static void afterAll() {
        TestUtils.deleteNamespace(client, TEST_NAMESPACE);
        client.close();
    }

    @AfterEach
    void teardown() {
        client.pods().inNamespace(TEST_NAMESPACE).resource(pod).delete();
    }

    @Test
    void eventPublicationSucceeds() {
        KubernetesRestartEventPublisher publisher = new KubernetesRestartEventPublisher(client, "op") { };
        Reconciliation reconciliation = new Reconciliation("test", Kafka.RESOURCE_KIND, TEST_NAMESPACE, "test");
        publisher.publishRestartEvents(reconciliation, pod, RestartReasons.of(RestartReason.CLUSTER_CA_CERT_KEY_REPLACED).add(RestartReason.FILE_SYSTEM_RESIZE_NEEDED));

        ListOptions strimziEventsOnly = new ListOptionsBuilder()
                .withFieldSelector("reportingController=" + KubernetesRestartEventPublisher.CONTROLLER)
                .build();
        List<Event> items = client.events().v1().events().inNamespace(TEST_NAMESPACE).list(strimziEventsOnly).getItems();
        assertThat(items, hasSize(2));
        assertThat(items.stream().map(Event::getReason).collect(toSet()), is(Set.of("ClusterCaCertKeyReplaced", "FileSystemResizeNeeded")));

        Event exemplar = items.get(0);
        assertThat(exemplar.getAction(), is(KubernetesRestartEventPublisher.ACTION));
        assertThat(exemplar.getRegarding(), is(referenceFromReconciliation(reconciliation)));
        assertThat(exemplar.getRelated(), is(referenceFromPod(pod)));
    }

    static ObjectReference referenceFromReconciliation(Reconciliation reconciliation) {
        return new ObjectReferenceBuilder()
                .withKind(reconciliation.kind())
                .withNamespace(reconciliation.namespace())
                .withName(reconciliation.name())
                .build();
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
