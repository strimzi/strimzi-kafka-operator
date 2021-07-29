/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource.publication.micrometer;

import io.fabric8.kubernetes.api.model.Pod;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.model.RestartReason;
import io.strimzi.operator.common.model.RestartReasons;
import io.strimzi.operator.common.operator.resource.publication.RestartEventsPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

class MicrometerRestartEventsPublisherTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    MetricsProvider provider;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    Pod pod;

    @Captor
    ArgumentCaptor<Tags> metricTags;

    @BeforeEach
    void setup() {
        initMocks(this);
        when(pod.getMetadata().getName()).thenReturn("example-pod");
        when(pod.getMetadata().getNamespace()).thenReturn("test-ns");
    }

    @Test
    void expectedFieldsPopulated() {
        RestartEventsPublisher micrometerPublisher = new MicrometerRestartEventsPublisher(provider);

        RestartReasons reasons = new RestartReasons().add(RestartReason.JBOD_VOLUMES_CHANGED);
        micrometerPublisher.publishRestartEvents(pod, reasons);

        verify(provider).counter(eq("strimzi.initiated.pod.restarts"), eq("Pod restarts initiated by the Strimzi cluster operator"), metricTags.capture());

        Set<Tag> tags = metricTags.getValue().stream().collect(Collectors.toSet());
        assertThat(tags.size(), is(3));
        assertThat(Tag.of("pod.name", "example-pod"), is(in(tags)));
        assertThat(Tag.of("pod.namespace", "test-ns"), is(in(tags)));
        assertThat(Tag.of("reason", "jbod_volumes_changed"), is(in(tags)));
    }

    @Test
    void eachReasonHasOwnLabelValue() {
        RestartEventsPublisher micrometerPublisher = new MicrometerRestartEventsPublisher(provider);

        RestartReasons reasons = new RestartReasons().add(RestartReason.JBOD_VOLUMES_CHANGED)
                                                     .add(RestartReason.CA_CERT_REMOVED);

        micrometerPublisher.publishRestartEvents(pod, reasons);

        //I expect this method to be called twice
        verify(provider, times(2)).counter(anyString(), anyString(), metricTags.capture());

        List<Tag> allTags = metricTags.getAllValues().stream().flatMap(Tags::stream).collect(Collectors.toList());

        assertThat("Three tags per invocation, pod name, pod ns, reason", allTags.size(), is(6));
        assertThat("When duplicated tags removed, expect 2 common, and 2 unique", new HashSet<>(allTags).size(), is(4));
        assertThat(Tag.of("reason",  "jbod_volumes_changed"), is(in(allTags)));
        assertThat(Tag.of("reason",  "ca_cert_removed"), is(in(allTags)));
    }

}