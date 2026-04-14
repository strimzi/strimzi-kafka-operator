/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.api.kafka.model.podset.StrimziPodSetBuilder;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class PodRevisionTest {
    private static final Pod POD = new PodBuilder()
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
                            .withResources(new ResourceRequirementsBuilder()
                                    .withRequests(Map.of("cpu", new Quantity("1"), "memory", new Quantity("1024Mi")))
                                    .withLimits(Map.of("cpu", new Quantity("2"), "memory", new Quantity("2048Mi")))
                                    .build())
                            .build())
                    .withRestartPolicy("Always")
                    .withTerminationGracePeriodSeconds(0L)
                .endSpec()
                .build();

    @Test
    public void testRevisions() {
        String basicPodRevision = PodRevision.getRevision(Reconciliation.DUMMY_RECONCILIATION, POD);

        // The same pod has always the same revision
        assertThat(PodRevision.getRevision(Reconciliation.DUMMY_RECONCILIATION, POD), is(basicPodRevision));
        assertThat(PodRevision.getRevision(Reconciliation.DUMMY_RECONCILIATION, new PodBuilder(POD).build()), is(basicPodRevision));

        // Revision does not change when resources change
        Pod pod2 = new PodBuilder(POD)
                .editSpec()
                    .editContainer(0)
                        .withResources(new ResourceRequirementsBuilder()
                                .withRequests(Map.of("cpu", new Quantity("1"), "memory", new Quantity("1024Mi")))
                                .withLimits(Map.of("cpu", new Quantity("3"), "memory", new Quantity("4096Mi")))
                                .build())
                    .endContainer()
                .endSpec()
                .build();
        assertThat(PodRevision.getRevision(Reconciliation.DUMMY_RECONCILIATION, pod2), is(basicPodRevision));

        // Different pods have different revisions
        pod2 = new PodBuilder(POD)
                .editSpec()
                    .withTerminationGracePeriodSeconds(30L)
                .endSpec()
                .build();
        assertThat(PodRevision.getRevision(Reconciliation.DUMMY_RECONCILIATION, pod2), is(not(basicPodRevision)));

        pod2 = new PodBuilder(POD)
                .editMetadata()
                    .withLabels(Map.of("app", "busybox"))
                .endMetadata()
                .build();
        assertThat(PodRevision.getRevision(Reconciliation.DUMMY_RECONCILIATION, pod2), is(not(basicPodRevision)));
    }

    @Test
    public void testResourceRevisions() {
        String basicPodRevision = PodRevision.getResourceRevision(Reconciliation.DUMMY_RECONCILIATION, POD);

        // The same pod has always the same revision
        assertThat(PodRevision.getResourceRevision(Reconciliation.DUMMY_RECONCILIATION, POD), is(basicPodRevision));
        assertThat(PodRevision.getResourceRevision(Reconciliation.DUMMY_RECONCILIATION, new PodBuilder(POD).build()), is(basicPodRevision));

        // When something else changes and not resources, the resource revision should not change
        Pod pod2 = new PodBuilder(POD)
                .editSpec()
                    .withTerminationGracePeriodSeconds(30L)
                .endSpec()
                .build();
        assertThat(PodRevision.getResourceRevision(Reconciliation.DUMMY_RECONCILIATION, pod2), is(basicPodRevision));

        // Revision changes when resources change
        pod2 = new PodBuilder(POD)
                .editSpec()
                    .editContainer(0)
                        .withResources(new ResourceRequirementsBuilder()
                                .withRequests(Map.of("cpu", new Quantity("1"), "memory", new Quantity("1024Mi")))
                                .withLimits(Map.of("cpu", new Quantity("3"), "memory", new Quantity("4096Mi")))
                                .build())
                    .endContainer()
                .endSpec()
                .build();
        assertThat(PodRevision.getResourceRevision(Reconciliation.DUMMY_RECONCILIATION, pod2), is(not(basicPodRevision)));
    }

    @Test
    public void testHasChangedWithPods()    {
        // Two pods without the revision annotation
        assertThat(PodRevision.hasChanged(POD, new PodBuilder(POD).build(), PodRevision.STRIMZI_REVISION_ANNOTATION), is(false));

        // Pods with the annotation
        Pod pod1 = new PodBuilder(POD)
                .editMetadata()
                    .withAnnotations(Map.of(PodRevision.STRIMZI_REVISION_ANNOTATION, "827d8b53"))
                .endMetadata()
                .build();
        Pod pod2 = new PodBuilder(POD)
                .editMetadata()
                    .withAnnotations(Map.of(PodRevision.STRIMZI_REVISION_ANNOTATION, "35db17f2"))
                .endMetadata()
                .build();

        assertThat(PodRevision.hasChanged(pod1, new PodBuilder(pod1).build(), PodRevision.STRIMZI_REVISION_ANNOTATION), is(false));
        assertThat(PodRevision.hasChanged(pod1, pod2, PodRevision.STRIMZI_REVISION_ANNOTATION), is(true));
        assertThat(PodRevision.hasChanged(POD, pod2, PodRevision.STRIMZI_REVISION_ANNOTATION), is(true));
    }

    @Test
    public void testHasChangedWithPodAndPodSet()    {
        // Two pods without the revision annotation
        assertThat(PodRevision.hasChanged(POD, podSet(POD), PodRevision.STRIMZI_REVISION_ANNOTATION), is(false));

        // Pods with the annotation
        Pod pod1 = new PodBuilder(POD)
                .editMetadata()
                    .withAnnotations(Map.of(PodRevision.STRIMZI_REVISION_ANNOTATION, "827d8b53"))
                .endMetadata()
                .build();
        Pod pod2 = new PodBuilder(POD)
                .editMetadata()
                    .withAnnotations(Map.of(PodRevision.STRIMZI_REVISION_ANNOTATION, "35db17f2"))
                .endMetadata()
                .build();
        Pod pod3 = new PodBuilder(POD)
                .editMetadata()
                    .withName("my-pod2")
                    .withAnnotations(Map.of(PodRevision.STRIMZI_REVISION_ANNOTATION, "35db17f2"))
                .endMetadata()
                .build();

        assertThat(PodRevision.hasChanged(pod1, podSet(pod1), PodRevision.STRIMZI_REVISION_ANNOTATION), is(false));
        assertThat(PodRevision.hasChanged(pod1, podSet(pod1, pod3), PodRevision.STRIMZI_REVISION_ANNOTATION), is(false));
        assertThat(PodRevision.hasChanged(pod1, podSet(pod2), PodRevision.STRIMZI_REVISION_ANNOTATION), is(true));
        assertThat(PodRevision.hasChanged(POD, podSet(pod2), PodRevision.STRIMZI_REVISION_ANNOTATION), is(true));
    }

    /**
     * Helper method to generate the StrimziPodSet resource
     *
     * @param pods  Pods which should be included in the resource
     *
     * @return      StrimziPodSet with the pods
     */
    private static StrimziPodSet podSet(Pod... pods)   {
        return new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName("my-podset")
                .endMetadata()
                .withNewSpec()
                    .withSelector(new LabelSelector(null, Map.of(Labels.STRIMZI_KIND_LABEL, "Kafka", Labels.STRIMZI_CLUSTER_LABEL, "my-kafka")))
                    .withPods(PodSetUtils.podsToMaps(Arrays.asList(pods)))
                .endSpec()
                .build();
    }
}
