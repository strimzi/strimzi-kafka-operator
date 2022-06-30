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
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.common.Reconciliation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.mockito.Mockito.when;

class KubernetesRestartEventPublisherTest {

    private KubernetesRestartEventPublisher publisher;

    @BeforeEach
    void setup() {
        publisher = new KubernetesRestartEventPublisher() {
            @Override
            protected void publishEvent(MicroTime eventTime, ObjectReference podReference, String reason, String type, String note) {
            }
        };
    }

    @Test
    void testVersionSpecificPublisherCreation() {
        KubernetesClient client = Mockito.mock(KubernetesClient.class);

        KubernetesRestartEventPublisher shouldBeV1 = KubernetesRestartEventPublisher.createPublisher(client, "",  true);
        assertThat(shouldBeV1, isA(V1RestartEventPublisher.class));

        KubernetesRestartEventPublisher shouldBeV1Beta1 = KubernetesRestartEventPublisher.createPublisher(client, "", false);
        assertThat(shouldBeV1Beta1, isA(V1Beta1RestartEventPublisher.class));
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
        Reconciliation mockReconciliation = Mockito.mock(Reconciliation.class);
        ObjectMeta mockPodMeta = new ObjectMetaBuilder().withName("pod").withNamespace("ns").build();
        when(mockPod.getMetadata()).thenReturn(mockPodMeta);


        Set<String> capturedReasons = new HashSet<>();
        KubernetesRestartEventPublisher capturingPublisher = new KubernetesRestartEventPublisher() {
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