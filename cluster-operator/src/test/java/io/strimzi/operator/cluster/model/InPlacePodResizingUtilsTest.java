/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodCondition;
import io.fabric8.kubernetes.api.model.PodConditionBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.api.kafka.model.podset.StrimziPodSetBuilder;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class InPlacePodResizingUtilsTest {
    @Test
    public void testResizingEnabledAnnotation() {
        assertThat(
                InPlacePodResizingUtils.inPlaceResizingEnabled(podWithAnnotation(Annotations.ANNO_STRIMZI_IO_IN_PLACE_RESIZING, "true")),
                is(true)
        );
        assertThat(
                InPlacePodResizingUtils.inPlaceResizingEnabled(podWithAnnotation(Annotations.ANNO_STRIMZI_IO_IN_PLACE_RESIZING, "false")),
                is(false)
        );
        assertThat(
                InPlacePodResizingUtils.inPlaceResizingEnabled(podWithAnnotation(Annotations.ANNO_STRIMZI_IO_IN_PLACE_RESIZING, "not-true")),
                is(false)
        );
        assertThat(
                InPlacePodResizingUtils.inPlaceResizingEnabled(podWithAnnotation("other-annotation", "true")),
                is(false)
        );
        assertThat(
                InPlacePodResizingUtils.inPlaceResizingEnabled(new PodBuilder().withNewMetadata().withName("test-pod").endMetadata().build()),
                is(false)
        );
    }

    @Test
    public void testWaitForDeferredAnnotation() {
        assertThat(
                InPlacePodResizingUtils.inPlaceResizingWaitForDeferred(podWithAnnotation(Annotations.ANNO_STRIMZI_IO_IN_PLACE_RESIZING_WAIT_FOR_DEFERRED, "true")),
                is(true)
        );
        assertThat(
                InPlacePodResizingUtils.inPlaceResizingWaitForDeferred(podWithAnnotation(Annotations.ANNO_STRIMZI_IO_IN_PLACE_RESIZING_WAIT_FOR_DEFERRED, "false")),
                is(false)
        );
        assertThat(
                InPlacePodResizingUtils.inPlaceResizingWaitForDeferred(podWithAnnotation(Annotations.ANNO_STRIMZI_IO_IN_PLACE_RESIZING_WAIT_FOR_DEFERRED, "not-true")),
                is(false)
        );
        assertThat(
                InPlacePodResizingUtils.inPlaceResizingWaitForDeferred(podWithAnnotation("other-annotation", "true")),
                is(false)
        );
        assertThat(
                InPlacePodResizingUtils.inPlaceResizingWaitForDeferred(new PodBuilder().withNewMetadata().withName("test-pod").endMetadata().build()),
                is(false)
        );
    }

    @Test
    public void testCanResourcesBeUpdatedInPlace()  {
        ResourceRequirements resources = new ResourceRequirementsBuilder()
                .withRequests(Map.of("cpu", new Quantity("1"), "memory", new Quantity("1024Mi")))
                .withLimits(Map.of("cpu", new Quantity("2"), "memory", new Quantity("2048Mi")))
                .build();
        ResourceRequirements resources2 = new ResourceRequirementsBuilder()
                .withRequests(Map.of("cpu", new Quantity("1"), "memory", new Quantity("1024Mi")))
                .withLimits(Map.of("cpu", new Quantity("3"), "memory", new Quantity("4096Mi")))
                .build();
        ResourceRequirements resourcesNoRequests = new ResourceRequirementsBuilder()
                .withLimits(Map.of("cpu", new Quantity("2"), "memory", new Quantity("2048Mi")))
                .build();
        ResourceRequirements resourcesNoLimits = new ResourceRequirementsBuilder()
                .withRequests(Map.of("cpu", new Quantity("1"), "memory", new Quantity("1024Mi")))
                .build();
        ResourceRequirements resourcesNoLimits2 = new ResourceRequirementsBuilder()
                .withRequests(Map.of("cpu", new Quantity("1"), "memory", new Quantity("2048Mi")))
                .build();
        ResourceRequirements resourcesNoCpuLimits = new ResourceRequirementsBuilder()
                .withRequests(Map.of("cpu", new Quantity("1"), "memory", new Quantity("1024Mi")))
                .withLimits(Map.of("memory", new Quantity("4096Mi")))
                .build();
        ResourceRequirements resourcesNoMemoryLimits = new ResourceRequirementsBuilder()
                .withRequests(Map.of("cpu", new Quantity("1"), "memory", new Quantity("1024Mi")))
                .withLimits(Map.of("cpu", new Quantity("3")))
                .build();

        // Single container -> resizing possible
        assertThat(InPlacePodResizingUtils.canResourcesBeUpdatedInPlace(podWithResources(null), podWithResources(null)), is(true));
        assertThat(InPlacePodResizingUtils.canResourcesBeUpdatedInPlace(podWithResources(resources), podWithResources(resources2)), is(true));
        assertThat(InPlacePodResizingUtils.canResourcesBeUpdatedInPlace(podWithResources(null), podWithResources(resources2)), is(true));
        assertThat(InPlacePodResizingUtils.canResourcesBeUpdatedInPlace(podWithResources(resourcesNoLimits), podWithResources(resources)), is(true));
        assertThat(InPlacePodResizingUtils.canResourcesBeUpdatedInPlace(podWithResources(resourcesNoLimits), podWithResources(resourcesNoLimits2)), is(true));
        assertThat(InPlacePodResizingUtils.canResourcesBeUpdatedInPlace(podWithResources(resourcesNoLimits), podWithResources(resourcesNoCpuLimits)), is(true));
        assertThat(InPlacePodResizingUtils.canResourcesBeUpdatedInPlace(podWithResources(resourcesNoLimits), podWithResources(resourcesNoMemoryLimits)), is(true));
        assertThat(InPlacePodResizingUtils.canResourcesBeUpdatedInPlace(podWithResources(resourcesNoRequests), podWithResources(resources)), is(true));

        // Multi-container -> resizing possible
        assertThat(InPlacePodResizingUtils.canResourcesBeUpdatedInPlace(podWithResourcesMultiContainer("c1", resources, "c2", resources), podWithResourcesMultiContainer("c1", resources2, "c2", resources2)), is(true));
        assertThat(InPlacePodResizingUtils.canResourcesBeUpdatedInPlace(podWithResourcesMultiContainer("c1", resourcesNoLimits, "c2", resources), podWithResourcesMultiContainer("c1", resources2, "c2", resources2)), is(true));

        // Single container -> resizing impossible
        assertThat(InPlacePodResizingUtils.canResourcesBeUpdatedInPlace(podWithResources(resources), podWithResources(null)), is(false));
        assertThat(InPlacePodResizingUtils.canResourcesBeUpdatedInPlace(podWithResources(resources), podWithResources(resourcesNoLimits)), is(false));
        assertThat(InPlacePodResizingUtils.canResourcesBeUpdatedInPlace(podWithResources(resources), podWithResources(resourcesNoCpuLimits)), is(false));
        assertThat(InPlacePodResizingUtils.canResourcesBeUpdatedInPlace(podWithResources(resources), podWithResources(resourcesNoMemoryLimits)), is(false));

        // Multi-container -> resizing impossible
        assertThat(InPlacePodResizingUtils.canResourcesBeUpdatedInPlace(podWithResourcesMultiContainer("c1", null, "c2", null), podWithResources(null)), is(false));
        assertThat(InPlacePodResizingUtils.canResourcesBeUpdatedInPlace(podWithResourcesMultiContainer("c1", resources, "c2", resources), podWithResources(resources2)), is(false));
        assertThat(InPlacePodResizingUtils.canResourcesBeUpdatedInPlace(podWithResourcesMultiContainer("c1", resources, "c2", resources), podWithResourcesMultiContainer("c1", resources2, "c3", resources2)), is(false));
        assertThat(InPlacePodResizingUtils.canResourcesBeUpdatedInPlace(podWithResourcesMultiContainer("c1", resources, "c2", resources), podWithResourcesMultiContainer("c1", resources2, "c2", resourcesNoLimits)), is(false));
        assertThat(InPlacePodResizingUtils.canResourcesBeUpdatedInPlace(podWithResourcesMultiContainer("c1", resources, "c2", resources), podWithResourcesMultiContainer("c1", resources2, "c2", null)), is(false));
    }

    @Test
    public void testPatchPodResources() {
        ResourceRequirements resources = new ResourceRequirementsBuilder()
                .withRequests(Map.of("cpu", new Quantity("1"), "memory", new Quantity("1024Mi")))
                .withLimits(Map.of("cpu", new Quantity("2"), "memory", new Quantity("2048Mi")))
                .build();
        ResourceRequirements resources2 = new ResourceRequirementsBuilder()
                .withRequests(Map.of("cpu", new Quantity("1"), "memory", new Quantity("1024Mi")))
                .withLimits(Map.of("cpu", new Quantity("3"), "memory", new Quantity("4096Mi")))
                .build();
        ResourceRequirements resources3 = new ResourceRequirementsBuilder()
                .withRequests(Map.of("cpu", new Quantity("1"), "memory", new Quantity("1024Mi")))
                .withLimits(Map.of("cpu", new Quantity("3"), "memory", new Quantity("8192Mi")))
                .build();

        // Should be pathed
        Pod patchedPod = InPlacePodResizingUtils.patchPodResources(podWithResourcesMultiContainer("c1", resources, "c2", resources), podWithResourcesMultiContainer("c1", resources2, "c2", resources3));
        assertThat(patchedPod.getSpec().getContainers().size(), is(2));
        assertThat(patchedPod.getSpec().getContainers().get(0).getResources(), is(resources2));
        assertThat(patchedPod.getSpec().getContainers().get(1).getResources(), is(resources3));

        // Different containers
        RuntimeException e = assertThrows(RuntimeException.class, () -> InPlacePodResizingUtils.patchPodResources(podWithResourcesMultiContainer("c1", resources, "c2", resources), podWithResourcesMultiContainer("c1", resources2, "c3", resources2)));
        assertThat(e.getMessage(), is("Cannot patch pod resources: container c2 is missing in the desired Pod"));
    }

    @Test
    public void testIsRestartNeeded() {
        assertThat(InPlacePodResizingUtils.restartForResourceResizingNeeded(Reconciliation.DUMMY_RECONCILIATION, podWithConditions(List.of(podCondition("PodResizePending", "True", "Deferred"))), true), is(false));
        assertThat(InPlacePodResizingUtils.restartForResourceResizingNeeded(Reconciliation.DUMMY_RECONCILIATION, podWithConditions(List.of(podCondition("PodResizePending", "True", "Deferred"))), false), is(true));

        assertThat(InPlacePodResizingUtils.restartForResourceResizingNeeded(Reconciliation.DUMMY_RECONCILIATION, podWithConditions(List.of(podCondition("PodResizePending", "True", "Infeasible"))), true), is(true));
        assertThat(InPlacePodResizingUtils.restartForResourceResizingNeeded(Reconciliation.DUMMY_RECONCILIATION, podWithConditions(List.of(podCondition("PodResizePending", "True", "Infeasible"))), false), is(true));

        assertThat(InPlacePodResizingUtils.restartForResourceResizingNeeded(Reconciliation.DUMMY_RECONCILIATION, podWithConditions(List.of(podCondition("PodResizeInProgress", "True", "Error"))), true), is(true));
        assertThat(InPlacePodResizingUtils.restartForResourceResizingNeeded(Reconciliation.DUMMY_RECONCILIATION, podWithConditions(List.of(podCondition("PodResizeInProgress", "True", "Error"))), false), is(true));

        // Different conditions
        assertThat(InPlacePodResizingUtils.restartForResourceResizingNeeded(Reconciliation.DUMMY_RECONCILIATION, podWithConditions(List.of(podCondition("SomeOtherCondition", "True", "Error"))), true), is(false));
        assertThat(InPlacePodResizingUtils.restartForResourceResizingNeeded(Reconciliation.DUMMY_RECONCILIATION, podWithConditions(List.of(podCondition("SomeOtherCondition", "True", "Error"))), false), is(false));

        // Multiple conditions
        assertThat(InPlacePodResizingUtils.restartForResourceResizingNeeded(Reconciliation.DUMMY_RECONCILIATION, podWithConditions(List.of(podCondition("SomeOtherCondition", "True", "Error"), podCondition("PodResizePending", "True", "Deferred"))), true), is(false));
        assertThat(InPlacePodResizingUtils.restartForResourceResizingNeeded(Reconciliation.DUMMY_RECONCILIATION, podWithConditions(List.of(podCondition("SomeOtherCondition", "True", "Error"), podCondition("PodResizePending", "True", "Deferred"))), false), is(true));

        // No conditions
        assertThat(InPlacePodResizingUtils.restartForResourceResizingNeeded(Reconciliation.DUMMY_RECONCILIATION, podWithConditions(List.of()), true), is(false));
        assertThat(InPlacePodResizingUtils.restartForResourceResizingNeeded(Reconciliation.DUMMY_RECONCILIATION, podWithConditions(List.of()), false), is(false));

    }

    @Test
    public void testInPlaceResourceResizingRollingUpdate()   {
        ResourceRequirements resources = new ResourceRequirementsBuilder()
                .withRequests(Map.of("cpu", new Quantity("1"), "memory", new Quantity("1024Mi")))
                .withLimits(Map.of("cpu", new Quantity("2")))
                .build();
        ResourceRequirements resources2 = new ResourceRequirementsBuilder()
                .withRequests(Map.of("cpu", new Quantity("1"), "memory", new Quantity("1024Mi")))
                .withLimits(Map.of("cpu", new Quantity("3")))
                .build();
        ResourceRequirements resources3 = new ResourceRequirementsBuilder()
                .withRequests(Map.of("cpu", new Quantity("1"), "memory", new Quantity("1024Mi")))
                .withLimits(Map.of("cpu", new Quantity("3"), "memory", new Quantity("4096Mi")))
                .build();

        Pod pod1 = podWithAnnotationResourcesPhaseAndCondition(PodRevision.STRIMZI_RESOURCE_REVISION_ANNOTATION, "avfc1874", resources, "Running", List.of());
        Pod pod2 = podWithAnnotationResourcesPhaseAndCondition(PodRevision.STRIMZI_RESOURCE_REVISION_ANNOTATION, "skso1919", resources2, "Running", List.of());
        Pod pod3 = podWithAnnotationResourcesPhaseAndCondition(PodRevision.STRIMZI_RESOURCE_REVISION_ANNOTATION, "skso1919", resources2, "Pending", List.of(podCondition("PodScheduled", "False", "Unschedulable")));
        Pod pod4 = podWithAnnotationResourcesPhaseAndCondition(PodRevision.STRIMZI_RESOURCE_REVISION_ANNOTATION, "skso1919", resources3, "Running", List.of());
        Pod pod5 = podWithAnnotationResourcesPhaseAndCondition(PodRevision.STRIMZI_RESOURCE_REVISION_ANNOTATION, "skso1919", resources2, "Running", List.of(podCondition("PodResizePending", "True", "Deferred")));
        Pod pod6 = podWithAnnotationResourcesPhaseAndCondition(PodRevision.STRIMZI_RESOURCE_REVISION_ANNOTATION, "skso1919", resources2, "Running", List.of(podCondition("PodResizePending", "True", "Infeasible")));
        Pod pod7 = new PodBuilder(pod1)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_IN_PLACE_RESIZING_ERROR, "true")
                .endMetadata()
                .build();
        Pod pod8 = new PodBuilder(pod2)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_IN_PLACE_RESIZING_ERROR, "true")
                .endMetadata()
                .build();

        @SuppressWarnings("unchecked")
        StrimziPodSet podSet = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName("test-sps")
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_IN_PLACE_RESIZING, "true"))
                .endMetadata()
                .withNewSpec()
                    .withPods(PodSetUtils.podToMap(pod1))
                .endSpec()
                .build();

        assertThat(restartReasons(podSet, pod1).shouldRestart(), is(false));
        assertThat(restartReasons(podSet, pod2).shouldRestart(), is(false));

        // Pod is not running and revision changed
        assertThat(restartReasons(podSet, pod3).getReasons(), is(Set.of(RestartReason.POD_HAS_OLD_RESOURCE_REVISION)));

        // Memory Limit was removed -> cannot be changed in place
        assertThat(restartReasons(podSet, pod4).getReasons(), is(Set.of(RestartReason.POD_HAS_OLD_RESOURCE_REVISION)));

        // Deferred -> should be rolled
        assertThat(restartReasons(podSet, pod5).getReasons(), is(Set.of(RestartReason.POD_RESOURCES_CHANGED)));

        // Infeasible -> should be rolled
        assertThat(restartReasons(podSet, pod6).getReasons(), is(Set.of(RestartReason.POD_RESOURCES_CHANGED)));

        // It has the resizing error annotation, but the resources are not changes => No restart
        assertThat(restartReasons(podSet, pod7).shouldRestart(), is(false));

        // StrimziPodSetController failed to resize the pod and the resource annotations differ => Restart Pod
        assertThat(restartReasons(podSet, pod8).getReasons(), is(Set.of(RestartReason.POD_RESOURCES_CHANGED)));
    }

    @Test
    public void testInPlaceResourceResizingRollingUpdateAndWaitingForDeferred()   {
        ResourceRequirements resources = new ResourceRequirementsBuilder()
                .withRequests(Map.of("cpu", new Quantity("1"), "memory", new Quantity("1024Mi")))
                .withLimits(Map.of("cpu", new Quantity("2")))
                .build();
        ResourceRequirements resources2 = new ResourceRequirementsBuilder()
                .withRequests(Map.of("cpu", new Quantity("1"), "memory", new Quantity("1024Mi")))
                .withLimits(Map.of("cpu", new Quantity("3")))
                .build();

        Pod pod1 = podWithAnnotationResourcesPhaseAndCondition(PodRevision.STRIMZI_RESOURCE_REVISION_ANNOTATION, "avfc1874", resources, "Running", List.of());
        Pod pod2 = podWithAnnotationResourcesPhaseAndCondition(PodRevision.STRIMZI_RESOURCE_REVISION_ANNOTATION, "skso1919", resources2, "Running", List.of(podCondition("PodResizePending", "True", "Deferred")));
        Pod pod3 = podWithAnnotationResourcesPhaseAndCondition(PodRevision.STRIMZI_RESOURCE_REVISION_ANNOTATION, "skso1919", resources2, "Running", List.of(podCondition("PodResizePending", "True", "Infeasible")));

        @SuppressWarnings("unchecked")
        StrimziPodSet podSet = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName("test-sps")
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_IN_PLACE_RESIZING, "true", Annotations.ANNO_STRIMZI_IO_IN_PLACE_RESIZING_WAIT_FOR_DEFERRED, "true"))
                .endMetadata()
                .withNewSpec()
                    .withPods(PodSetUtils.podToMap(pod1))
                .endSpec()
                .build();

        // The same Pod -> deferred should make no difference
        assertThat(restartReasons(podSet, pod1).shouldRestart(), is(false));

        // Deferred -> should not be rolled
        assertThat(restartReasons(podSet, pod2).shouldRestart(), is(false));

        // Infeasible -> should be rolled
        assertThat(restartReasons(podSet, pod3).getReasons(), is(Set.of(RestartReason.POD_RESOURCES_CHANGED)));
    }

    //////////////////////////////
    // Utility methods
    //////////////////////////////

    private static Pod podWithAnnotation(String annotationKey, String annotationValue)  {
        return new PodBuilder()
                .withNewMetadata()
                    .withName("test-pod")
                    .addToAnnotations(annotationKey, annotationValue)
                .endMetadata()
                .build();
    }

    private static Pod podWithResources(ResourceRequirements resources)  {
        return new PodBuilder()
                .withNewMetadata()
                    .withName("test-pod")
                .endMetadata()
                .withNewSpec()
                    .addNewContainer()
                        .withName("c1")
                        .withResources(resources)
                    .endContainer()
                .endSpec()
                .build();
    }

    private static Pod podWithAnnotationResourcesPhaseAndCondition(String annotationKey, String annotationValue, ResourceRequirements resources, String phase, List<PodCondition> conditions)  {
        return new PodBuilder()
                .withNewMetadata()
                    .withName("test-pod")
                    .addToAnnotations(annotationKey, annotationValue)
                .endMetadata()
                .withNewSpec()
                    .addNewContainer()
                        .withName("c1")
                        .withResources(resources)
                    .endContainer()
                .endSpec()
                .withNewStatus()
                    .withPhase(phase)
                    .withConditions(conditions)
                .endStatus()
                .build();
    }

    private static Pod podWithResourcesMultiContainer(String containerName1, ResourceRequirements resources1, String containerName2, ResourceRequirements resources2)  {
        return new PodBuilder()
                .withNewMetadata()
                    .withName("test-pod")
                .endMetadata()
                .withNewSpec()
                    .addNewContainer()
                        .withName(containerName1)
                        .withResources(resources1)
                    .endContainer()
                    .addNewContainer()
                        .withName(containerName2)
                        .withResources(resources2)
                    .endContainer()
                .endSpec()
                .build();
    }

    private static PodCondition podCondition(String type, String status, String reason)    {
        return new PodConditionBuilder()
            .withType(type)
            .withStatus(status)
            .withReason(reason)
            .build();
    }

    private static Pod podWithConditions(List<PodCondition> conditions)  {
        return new PodBuilder()
                .withNewMetadata()
                    .withName("test-pod")
                .endMetadata()
                .withNewStatus()
                    .withConditions(conditions)
                .endStatus()
                .build();
    }

    private static RestartReasons restartReasons(StrimziPodSet podSet, Pod pod)  {
        RestartReasons restartReasons = new RestartReasons();
        InPlacePodResizingUtils.reasonsToRestart(Reconciliation.DUMMY_RECONCILIATION, restartReasons, podSet, pod);

        return restartReasons;
    }
}
