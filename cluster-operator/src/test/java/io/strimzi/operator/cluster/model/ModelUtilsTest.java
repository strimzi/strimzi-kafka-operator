/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.strimzi.api.kafka.model.kafka.EphemeralStorageBuilder;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.api.kafka.model.podset.StrimziPodSetBuilder;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.strimzi.operator.common.Util.parseMap;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling"})
@ParallelSuite
public class ModelUtilsTest {
    @ParallelTest
    public void testParseImageMap() {
        Map<String, String> m = parseMap(
                KafkaVersionTestUtils.LATEST_KAFKA_VERSION + "=" + KafkaVersionTestUtils.LATEST_KAFKA_IMAGE + "\n  " +
                        KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION + "=" + KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE + "\n ");
        assertThat(m.size(), is(2));
        assertThat(m.get(KafkaVersionTestUtils.LATEST_KAFKA_VERSION), is(KafkaVersionTestUtils.LATEST_KAFKA_IMAGE));
        assertThat(m.get(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION), is(KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE));

        m = parseMap(
                KafkaVersionTestUtils.LATEST_KAFKA_VERSION + "=" + KafkaVersionTestUtils.LATEST_KAFKA_IMAGE + "," +
                KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION + "=" + KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE);
        assertThat(m.size(), is(2));
        assertThat(m.get(KafkaVersionTestUtils.LATEST_KAFKA_VERSION), is(KafkaVersionTestUtils.LATEST_KAFKA_IMAGE));
        assertThat(m.get(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION), is(KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE));
        assertThat(m.get(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION), is(KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE));
    }

    @Test
    public void testAnnotationsOrLabelsImageMap() {
        Map<String, String> m = parseMap(" discovery.3scale.net=true");
        assertThat(m.size(), is(1));
        assertThat(m.get("discovery.3scale.net"), is("true"));

        m = parseMap(" discovery.3scale.net/scheme=http\n" +
                "        discovery.3scale.net/port=8080\n" +
                "        discovery.3scale.net/path=path/\n" +
                "        discovery.3scale.net/description-path=oapi/");
        assertThat(m.size(), is(4));
        assertThat(m.get("discovery.3scale.net/scheme"), is("http"));
        assertThat(m.get("discovery.3scale.net/port"), is("8080"));
        assertThat(m.get("discovery.3scale.net/path"), is("path/"));
        assertThat(m.get("discovery.3scale.net/description-path"), is("oapi/"));
    }

    @ParallelTest
    public void testStorageSerializationAndDeserialization()    {
        Storage jbod = new JbodStorageBuilder().withVolumes(
                new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("100Gi").build(),
                new PersistentClaimStorageBuilder().withStorageClass("gp2-st1").withDeleteClaim(true).withId(1).withSize("1000Gi").build())
                .build();

        Storage ephemeral = new EphemeralStorageBuilder().build();

        Storage persistent = new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("100Gi").build();

        assertThat(ModelUtils.decodeStorageFromJson(ModelUtils.encodeStorageToJson(jbod)), is(jbod));
        assertThat(ModelUtils.decodeStorageFromJson(ModelUtils.encodeStorageToJson(ephemeral)), is(ephemeral));
        assertThat(ModelUtils.decodeStorageFromJson(ModelUtils.encodeStorageToJson(persistent)), is(persistent));
    }

    @ParallelTest
    public void testCreateOwnerReference()   {
        Kafka owner = new KafkaBuilder()
                .withNewMetadata()
                    .withName("my-kafka")
                    .withUid("some-uid")
                .endMetadata()
                .build();

        OwnerReference ref = ModelUtils.createOwnerReference(owner, false);

        assertThat(ref.getApiVersion(), is(owner.getApiVersion()));
        assertThat(ref.getKind(), is(owner.getKind()));
        assertThat(ref.getName(), is(owner.getMetadata().getName()));
        assertThat(ref.getUid(), is(owner.getMetadata().getUid()));
        assertThat(ref.getBlockOwnerDeletion(), is(false));
        assertThat(ref.getController(), is(false));
    }

    @ParallelTest
    public void testCreateControllerOwnerReference()   {
        StrimziPodSet owner = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName("my-cluster-kafka")
                    .withUid("some-uid")
                .endMetadata()
                .build();

        OwnerReference ref = ModelUtils.createOwnerReference(owner, true);

        assertThat(ref.getApiVersion(), is(owner.getApiVersion()));
        assertThat(ref.getKind(), is(owner.getKind()));
        assertThat(ref.getName(), is(owner.getMetadata().getName()));
        assertThat(ref.getUid(), is(owner.getMetadata().getUid()));
        assertThat(ref.getBlockOwnerDeletion(), is(false));
        assertThat(ref.getController(), is(true));
    }

    @ParallelTest
    public void testHasOwnerReference()    {
        OwnerReference owner = new OwnerReferenceBuilder()
                .withApiVersion("my-api")
                .withKind("my-kind")
                .withName("my-owner")
                .withUid("a02c09d8-a04f-469d-97ba-920720abe9b3")
                .withBlockOwnerDeletion(false)
                .withController(false)
                .build();

        OwnerReference otherOwner = new OwnerReferenceBuilder()
                .withApiVersion("my-other-api")
                .withKind("my-other-kind")
                .withName("my-other-owner")
                .withUid("3dfcd6b9-ad05-4277-8d13-147346fe1f70")
                .withBlockOwnerDeletion(false)
                .withController(false)
                .build();

        Pod pod = new PodBuilder()
                    .withNewMetadata()
                        .withName("my-pod")
                        .withNamespace("my-namespace")
                    .endMetadata()
                    .withNewSpec()
                        .withContainers(new ContainerBuilder()
                                .withName("busybox")
                                .withImage("busybox")
                                .withCommand("sleep", "3600")
                                .withImagePullPolicy("IfNotPresent")
                                .build())
                        .withRestartPolicy("Always")
                        .withTerminationGracePeriodSeconds(0L)
                    .endSpec()
                    .build();

        // No owner reference
        assertThat(ModelUtils.hasOwnerReference(pod, owner), is(false));

        // Only our owner reference
        pod.getMetadata().setOwnerReferences(List.of(owner));
        assertThat(ModelUtils.hasOwnerReference(pod, owner), is(true));

        // Other owner reference
        pod.getMetadata().setOwnerReferences(List.of(otherOwner));
        assertThat(ModelUtils.hasOwnerReference(pod, owner), is(false));

        // Multiple owner references
        pod.getMetadata().setOwnerReferences(List.of(otherOwner, owner));
        assertThat(ModelUtils.hasOwnerReference(pod, owner), is(true));
    }



    @ParallelTest
    public void testServiceDnsNames() {
        List<String> dnsNames = ModelUtils.generateAllServiceDnsNames("my-namespace", "my-service");

        assertThat(dnsNames.size(), is(4));
        assertThat(dnsNames, hasItems("my-service", "my-service.my-namespace", "my-service.my-namespace.svc", "my-service.my-namespace.svc.cluster.local"));
    }
}
