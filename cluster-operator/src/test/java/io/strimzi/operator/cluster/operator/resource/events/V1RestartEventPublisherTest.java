/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.events;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.events.v1.Event;
import io.fabric8.kubernetes.api.model.events.v1.EventList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.EventingAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.V1EventingAPIGroupDSL;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class V1RestartEventPublisherTest {
    private final static String NAMESPACE = "test-ns";
    private final static String POD_NAME = "example-pod";

    @Test
    void testPopulatesExpectedFields() {
        Resource<Event> mockEventResource = mock(Resource.class);

        NonNamespaceOperation<Event, EventList, Resource<Event>> nonNamespaceOp = mock(NonNamespaceOperation.class);
        ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        when(nonNamespaceOp.resource(eventCaptor.capture())).thenReturn(mockEventResource);

        MixedOperation<Event, EventList, Resource<Event>> mixedOp = mock(MixedOperation.class);
        when(mixedOp.inNamespace(eq(NAMESPACE))).thenReturn(nonNamespaceOp);

        V1EventingAPIGroupDSL v1EventingAPIGroupDSL = mock(V1EventingAPIGroupDSL.class);
        when(v1EventingAPIGroupDSL.events()).thenReturn(mixedOp);

        EventingAPIGroupDSL eventingAPIGroupDSL = mock(EventingAPIGroupDSL.class);
        when(eventingAPIGroupDSL.v1()).thenReturn(v1EventingAPIGroupDSL);

        KubernetesClient client = mock(KubernetesClient.class);
        when(client.events()).thenReturn(eventingAPIGroupDSL);

        Pod pod = new PodBuilder()
                .withNewMetadata()
                    .withName(POD_NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .build();

        Clock clock = Clock.fixed(Instant.parse("2020-10-11T00:00:00Z"), ZoneId.of("UTC"));

        V1RestartEventPublisher eventPublisher = new V1RestartEventPublisher(clock, client, "cluster-operator-id");

        RestartReasons reasons = new RestartReasons().add(RestartReason.JBOD_VOLUMES_CHANGED);
        eventPublisher.publishRestartEvents(pod, reasons);

        verify(mockEventResource, times(1)).create();

        Event publishedEvent = eventCaptor.getValue();
        assertThat(publishedEvent.getRegarding().getKind(), is("Pod"));
        assertThat(publishedEvent.getRegarding().getName(), is(POD_NAME));
        assertThat(publishedEvent.getRegarding().getNamespace(), is(NAMESPACE));

        assertThat(publishedEvent.getReportingController(), is("strimzi.io/cluster-operator"));
        assertThat(publishedEvent.getReportingInstance(), is("cluster-operator-id"));

        assertThat(publishedEvent.getReason(), is("JbodVolumesChanged"));
        assertThat(publishedEvent.getAction(), is("StrimziInitiatedPodRestart"));
        assertThat(publishedEvent.getType(), is("Normal"));
        assertThat(publishedEvent.getNote(), is(RestartReason.JBOD_VOLUMES_CHANGED.getDefaultNote()));
        assertThat(publishedEvent.getEventTime().getTime(), is("2020-10-11T00:00:00.000000Z"));

    }
}