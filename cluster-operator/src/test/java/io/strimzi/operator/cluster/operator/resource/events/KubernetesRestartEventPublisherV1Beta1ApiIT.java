/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.events;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.events.v1beta1.Event;
import io.fabric8.kubernetes.api.model.events.v1beta1.EventList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@EnabledIf("clusterSupportsEventsApiV1Beta1")
public class KubernetesRestartEventPublisherV1Beta1ApiIT extends KubernetesRestartEventPublisherITBase {

    private static final String TEST_NAMESPACE = "v1beta1-test-ns";
    private static KubernetesClient kubeClient;
    private Pod pod;

    //Don't run for clusters 1.25+ as v1beta1 is removed
    static boolean clusterSupportsEventsApiV1Beta1() {
        return checkClusterVersionMatches((major, minor) -> major == 1 && minor < 25);
    }

    @BeforeAll
    static void beforeAll() {
        kubeClient = prepareNamespace(TEST_NAMESPACE);
    }

    @AfterAll
    static void afterAll() {
        teardownNamespace(TEST_NAMESPACE);
    }

    @BeforeEach
    void setup() {
        pod = createPod(TEST_NAMESPACE);
    }

    @AfterEach
    void teardown() {
        teardownPod(TEST_NAMESPACE, pod);
        eventOps().delete();
    }

    @Test
    void eventPublicationSucceeds() {
        KubernetesRestartEventPublisher publisher = KubernetesRestartEventPublisher.createPublisher(kubeClient, "op", false);
        publisher.publishRestartEvents(pod, RestartReasons.of(RestartReason.CLUSTER_CA_CERT_KEY_REPLACED)
                                                          .add(RestartReason.MANUAL_ROLLING_UPDATE));

        // Unable to use field selectors beyond name and namespace for events.k8s.1o/v1b1 API on the version of
        // Minikube currently on Azure Pipelines,
        List<Event> events = eventOps().list()
                                       .getItems()
                                       .stream()
                                       .filter(e -> KubernetesRestartEventPublisher.ACTION.equals(e.getAction()))
                                       .collect(Collectors.toList());

        assertThat(events, hasSize(2));
        assertThat(events.stream().map(Event::getReason).collect(toSet()), is(Set.of("ClusterCaCertKeyReplaced", "ManualRollingUpdate")));

        Event exemplar = events.get(0);
        assertThat(exemplar.getAction(), is("StrimziInitiatedPodRestart"));
        assertThat(exemplar.getRegarding(), is(referenceFromPod(pod)));
    }

    private NonNamespaceOperation<Event, EventList, Resource<Event>> eventOps() {
        return kubeClient.events().v1beta1().events().inNamespace(TEST_NAMESPACE);
    }
}
