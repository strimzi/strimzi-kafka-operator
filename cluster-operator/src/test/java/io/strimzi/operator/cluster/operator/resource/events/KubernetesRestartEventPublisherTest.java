/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.events;

import io.fabric8.kubernetes.api.model.MicroTime;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.ObjectReference;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.time.Clock;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KubernetesRestartEventPublisherTest {
    private final static String NAMESPACE = "test-ns";
    private final static String POD_NAME = "example-pod";

    private KubernetesRestartEventPublisher publisher;

    @BeforeEach
    void setup() {
        KubernetesClient client = mock(KubernetesClient.class);
        publisher = new KubernetesRestartEventPublisher(client, "op") {
            @Override
            protected void publishEvent(MicroTime eventTime, ObjectReference podReference, String reason, String type, String note) {
            }
        };
    }

    @Test
    void testObjectReferenceFromPod() {
        ObjectMeta podMeta = new ObjectMeta();
        podMeta.setName("cluster-kafka-0");
        podMeta.setNamespace("strimzi-kafka");

        Pod mockPod = Mockito.mock(Pod.class);
        when(mockPod.getMetadata()).thenReturn(podMeta);

        ObjectReference podRef = publisher.createPodReference(mockPod);

        assertThat(podRef.getName(), is("cluster-kafka-0"));
        assertThat(podRef.getNamespace(), is("strimzi-kafka"));
        assertThat(podRef.getKind(), is("Pod"));
    }

    @Test
    void testTruncation() {
        String underOneThousandBytes = "1".repeat(999);
        String oneThousandBytes = "2".repeat(1000);
        String twoThousandBytes = "3".repeat(2000);
            
        assertThat(publisher.maybeTruncated(underOneThousandBytes).getBytes(UTF_8).length, is(999));
        assertThat(publisher.maybeTruncated(underOneThousandBytes), is(underOneThousandBytes));

        assertThat(publisher.maybeTruncated(oneThousandBytes).getBytes(UTF_8).length, is(1000));
        assertThat(publisher.maybeTruncated(oneThousandBytes), is(oneThousandBytes));

        assertThat(publisher.maybeTruncated(twoThousandBytes).getBytes(UTF_8).length, is(1000));
        assertThat(publisher.maybeTruncated(twoThousandBytes), is("3".repeat(997) + "..."));
    }

    @Test
    void testTruncationThrowsForMultibyteCharacters() {
        String note = "pound sign is encoded as 2 bytes in UTF-8 so here is the £";
        Assertions.assertThrows(UnsupportedOperationException.class, () -> publisher.maybeTruncated(note));
    }


    @Test
    void testOneEventPublishedPerReason() {
        Pod mockPod = Mockito.mock(Pod.class);
        ObjectMeta mockPodMeta = new ObjectMetaBuilder().withName("pod").withNamespace("ns").build();
        when(mockPod.getMetadata()).thenReturn(mockPodMeta);


        KubernetesClient client = mock(KubernetesClient.class);

        Set<String> capturedReasons = new HashSet<>();
        KubernetesRestartEventPublisher capturingPublisher = new KubernetesRestartEventPublisher(client, "op") {
            @Override
            protected void publishEvent(MicroTime eventTime, ObjectReference podReference, String reason, String type, String note) {
                capturedReasons.add(reason);
            }
        };

        Set<String> expectedReasons = Set.of("ClientCaCertKeyReplaced", "ClusterCaCertKeyReplaced");

        RestartReasons reasons = new RestartReasons().add(RestartReason.CLIENT_CA_CERT_KEY_REPLACED)
                                                     .add(RestartReason.CLUSTER_CA_CERT_KEY_REPLACED);

        capturingPublisher.publishRestartEvents(mockPod, reasons);

        assertThat(capturedReasons, is(expectedReasons));


    }

    @Test
    void testPopulatesExpectedFields() {
        @SuppressWarnings("unchecked")
        Resource<Event> mockEventResource = mock(Resource.class);

        @SuppressWarnings("unchecked")
        NonNamespaceOperation<Event, EventList, Resource<Event>> nonNamespaceOp = mock(NonNamespaceOperation.class);
        ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        when(nonNamespaceOp.resource(eventCaptor.capture())).thenReturn(mockEventResource);

        @SuppressWarnings("unchecked")
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

        Clock clock = Clock.fixed(Instant.parse("2020-10-11T00:00:00Z"), Clock.systemUTC().getZone());

        KubernetesRestartEventPublisher eventPublisher = new KubernetesRestartEventPublisher(client, "cluster-operator-id", clock);

        RestartReasons reasons = new RestartReasons().add(RestartReason.FILE_SYSTEM_RESIZE_NEEDED);
        eventPublisher.publishRestartEvents(pod, reasons);

        verify(mockEventResource, times(1)).create();

        Event publishedEvent = eventCaptor.getValue();
        assertThat(publishedEvent.getRegarding().getKind(), is("Pod"));
        assertThat(publishedEvent.getRegarding().getName(), is(POD_NAME));
        assertThat(publishedEvent.getRegarding().getNamespace(), is(NAMESPACE));

        assertThat(publishedEvent.getReportingController(), is("strimzi.io/cluster-operator"));
        assertThat(publishedEvent.getReportingInstance(), is("cluster-operator-id"));

        assertThat(publishedEvent.getReason(), is("FileSystemResizeNeeded"));
        assertThat(publishedEvent.getAction(), is("StrimziInitiatedPodRestart"));
        assertThat(publishedEvent.getType(), is("Normal"));
        assertThat(publishedEvent.getNote(), is(RestartReason.FILE_SYSTEM_RESIZE_NEEDED.getDefaultNote()));
        assertThat(publishedEvent.getEventTime().getTime(), is("2020-10-11T00:00:00.000000Z"));

    }
}