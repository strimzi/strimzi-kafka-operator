/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.api.kafka.model.podset.StrimziPodSetBuilder;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.Arrays;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

@ParallelSuite
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
                            .build())
                    .withRestartPolicy("Always")
                    .withTerminationGracePeriodSeconds(0L)
                .endSpec()
                .build();

    @ParallelTest
    public void testRevisions() {
        String basicPodRevision = PodRevision.getRevision(Reconciliation.DUMMY_RECONCILIATION, POD);

        // The same pod has always the same revision
        assertThat(PodRevision.getRevision(Reconciliation.DUMMY_RECONCILIATION, POD), is(basicPodRevision));
        assertThat(PodRevision.getRevision(Reconciliation.DUMMY_RECONCILIATION, new PodBuilder(POD).build()), is(basicPodRevision));

        // Different pods have different revisions
        Pod pod2 = new PodBuilder(POD)
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

    @ParallelTest
    public void testHasChangedWithPods()    {
        // Two pods without the revision annotation
        assertThat(PodRevision.hasChanged(POD, new PodBuilder(POD).build()), is(false));

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

        assertThat(PodRevision.hasChanged(pod1, new PodBuilder(pod1).build()), is(false));
        assertThat(PodRevision.hasChanged(pod1, pod2), is(true));
        assertThat(PodRevision.hasChanged(POD, pod2), is(true));
    }

    @ParallelTest
    public void testHasChangedWithPodAndPodSet()    {
        // Two pods without the revision annotation
        assertThat(PodRevision.hasChanged(POD, podSet(POD)), is(false));

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

        assertThat(PodRevision.hasChanged(pod1, podSet(pod1)), is(false));
        assertThat(PodRevision.hasChanged(pod1, podSet(pod1, pod3)), is(false));
        assertThat(PodRevision.hasChanged(pod1, podSet(pod2)), is(true));
        assertThat(PodRevision.hasChanged(POD, podSet(pod2)), is(true));
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
