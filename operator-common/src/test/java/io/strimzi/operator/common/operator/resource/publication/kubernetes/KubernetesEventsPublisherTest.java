/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource.publication.kubernetes;

import io.fabric8.kubernetes.api.model.MicroTime;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.PlatformFeaturesAvailability.EventApiVersion;
import io.strimzi.operator.common.model.RestartReason;
import io.strimzi.operator.common.model.RestartReasons;
import io.strimzi.operator.common.operator.resource.publication.RestartEventsPublisher;
import io.strimzi.operator.common.operator.resource.publication.kubernetes.versions.CoreEventPublisher;
import io.strimzi.operator.common.operator.resource.publication.kubernetes.versions.V1Beta1EventPublisher;
import io.strimzi.operator.common.operator.resource.publication.kubernetes.versions.V1EventPublisher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.Set;

import static io.strimzi.operator.common.model.RestartReason.ADMIN_CLIENT_CANNOT_CONNECT_TO_BROKER;
import static io.strimzi.operator.common.model.RestartReason.KAFKA_EXPORTER_CERTS_CHANGED;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.mockito.Mockito.when;

class KubernetesEventsPublisherTest {

    private KubernetesEventsPublisher publisher;

    @BeforeEach
    void setup() {
        publisher = new KubernetesEventsPublisher() {
            @Override
            protected void publishEvent(MicroTime eventTime, ObjectReference podReference, String reason, String type, String note) {
            }
        };
    }

    @Test
    void testVersionSpecificPublisherCreation() {
        KubernetesClient client = Mockito.mock(KubernetesClient.class);

        RestartEventsPublisher shouldBeV1 = KubernetesEventsPublisher.createPublisher(client, "", EventApiVersion.V1);
        assertThat(shouldBeV1, isA(V1EventPublisher.class));

        RestartEventsPublisher shouldBeV1Beta1 = KubernetesEventsPublisher.createPublisher(client, "", EventApiVersion.V1BETA1);
        assertThat(shouldBeV1Beta1, isA(V1Beta1EventPublisher.class));

        RestartEventsPublisher shouldBeCore = KubernetesEventsPublisher.createPublisher(client, "", EventApiVersion.CORE);
        assertThat(shouldBeCore, isA(CoreEventPublisher.class));
    }

    @Test
    void testReasonFormattedAsPascalCase() {
        assertThat(publisher.pascalCasedReason(ADMIN_CLIENT_CANNOT_CONNECT_TO_BROKER), is("AdminClientCannotConnectToBroker"));
        assertThat(publisher.pascalCasedReason(KAFKA_EXPORTER_CERTS_CHANGED), is("KafkaExporterCertsChanged"));
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

        Set<String> capturedReasons = new HashSet<>();
        KubernetesEventsPublisher capturingPublisher = new KubernetesEventsPublisher() {
            @Override
            protected void publishEvent(MicroTime eventTime, ObjectReference podReference, String reason, String type, String note) {
                capturedReasons.add(reason);
            }
        };

        Set<String> expectedReasons = Set.of("ClientCaCertKeyReplaced", "ClusterCaCertKeyReplaced", "CustomListenerCaCertChange");

        RestartReasons reasons = new RestartReasons().add(RestartReason.CLIENT_CA_CERT_KEY_REPLACED)
                                                     .add(RestartReason.CLUSTER_CA_CERT_KEY_REPLACED)
                                                     .add(RestartReason.CUSTOM_LISTENER_CA_CERT_CHANGE);

        capturingPublisher.publishRestartEvents(mockPod, reasons);

        assertThat(capturedReasons, is(expectedReasons));


    }
}