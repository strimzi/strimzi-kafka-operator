/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource.publication.kubernetes.versions;


import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.events.v1.Event;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.common.model.RestartReason;
import io.strimzi.operator.common.model.RestartReasons;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

class V1EventPublisherTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    Pod pod;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    KubernetesClient client;

    @Captor
    ArgumentCaptor<Event> eventCaptor;

    private Clock clock;

    @BeforeEach
    void setup() {
        initMocks(this);
        when(pod.getMetadata().getName()).thenReturn("example-pod");
        when(pod.getMetadata().getNamespace()).thenReturn("test-ns");
        clock = Clock.fixed(Instant.parse("2020-10-11T00:00:00Z"), ZoneId.of("UTC"));
    }

    @Test
    void testPopulatesExpectedFields() {
        V1EventPublisher eventPublisher = new V1EventPublisher(clock, client, "cluster-operator-id");

        RestartReasons reasons = new RestartReasons().add(RestartReason.JBOD_VOLUMES_CHANGED);
        eventPublisher.publishRestartEvents(pod, reasons);

        verify(client.events().v1().events()).create(eventCaptor.capture());

        Event publishedEvent = eventCaptor.getValue();
        assertThat(publishedEvent.getRegarding().getKind(), is("Pod"));
        assertThat(publishedEvent.getRegarding().getName(), is("example-pod"));
        assertThat(publishedEvent.getRegarding().getNamespace(), is("test-ns"));

        assertThat(publishedEvent.getReportingController(), is("strimzi.io/cluster-operator"));
        assertThat(publishedEvent.getReportingInstance(), is("cluster-operator-id"));

        assertThat(publishedEvent.getReason(), is("JbodVolumesChanged"));
        assertThat(publishedEvent.getAction(), is("StrimziInitiatedPodRestart"));
        assertThat(publishedEvent.getType(), is("Normal"));
        assertThat(publishedEvent.getNote(), is(RestartReason.JBOD_VOLUMES_CHANGED.getDefaultNote()));
        assertThat(publishedEvent.getEventTime().getTime(), is("2020-10-11T00:00:00.000000Z"));

    }
}