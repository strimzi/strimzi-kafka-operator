/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the {@link PodDiff} utility class.
 */
public class PodDiffTest {

    private static final String NAMESPACE = "test-namespace";
    private static final String POD_NAME = "test-pod";
    private static final String REVISION_ANNOTATION = PodRevision.STRIMZI_REVISION_ANNOTATION;

    private Pod createPod(Map<String, String> labels, Map<String, String> annotations, String revision) {
        return new PodBuilder()
                .withNewMetadata()
                    .withName(POD_NAME)
                    .withNamespace(NAMESPACE)
                    .withLabels(labels)
                    .withAnnotations(annotations != null 
                        ? mergeMaps(annotations, Map.of(REVISION_ANNOTATION, revision))
                        : Map.of(REVISION_ANNOTATION, revision))
                .endMetadata()
                .withNewSpec()
                    .addNewContainer()
                        .withName("kafka")
                        .withImage("quay.io/strimzi/kafka:latest")
                    .endContainer()
                .endSpec()
                .build();
    }

    private Map<String, String> mergeMaps(Map<String, String> m1, Map<String, String> m2) {
        java.util.Map<String, String> result = new java.util.HashMap<>(m1);
        result.putAll(m2);
        return result;
    }

    @Test
    public void testNoChanges() {
        String revision = "abc123";
        Pod current = createPod(Map.of("app", "kafka"), Map.of("custom", "value"), revision);
        Pod desired = createPod(Map.of("app", "kafka"), Map.of("custom", "value"), revision);

        PodDiff diff = PodDiff.diff(current, desired);

        assertThat(diff.getChangeType(), is(PodDiff.ChangeType.NONE));
        assertFalse(diff.hasChanges());
        assertFalse(diff.isPatchable());
        assertThat(diff.getTotalChangeCount(), is(0));
        assertTrue(diff.getLabelChanges().isEmpty());
        assertTrue(diff.getAnnotationChanges().isEmpty());
    }

    @Test
    public void testNoChangesSingletonReuse() {
        String revision = "abc123";
        Pod current = createPod(Map.of("app", "kafka"), Map.of("custom", "value"), revision);
        Pod desired = createPod(Map.of("app", "kafka"), Map.of("custom", "value"), revision);

        PodDiff diff1 = PodDiff.diff(current, desired);
        PodDiff diff2 = PodDiff.diff(current, desired);

        // Should return same singleton instance for no-change case
        assertThat(diff1, sameInstance(diff2));
    }

    @Test
    public void testLabelAdded() {
        String revision = "abc123";
        Pod current = createPod(Map.of("app", "kafka"), Map.of(), revision);
        Pod desired = createPod(Map.of("app", "kafka", "environment", "prod"), Map.of(), revision);

        PodDiff diff = PodDiff.diff(current, desired);

        assertThat(diff.getChangeType(), is(PodDiff.ChangeType.METADATA_ONLY));
        assertTrue(diff.hasChanges());
        assertTrue(diff.isPatchable());
        assertThat(diff.getTotalChangeCount(), is(1));
        assertThat(diff.getLabelChanges(), is(Map.of("environment", "prod")));
        assertTrue(diff.getAnnotationChanges().isEmpty());
    }

    @Test
    public void testLabelModified() {
        String revision = "abc123";
        Pod current = createPod(Map.of("app", "kafka", "version", "1.0"), Map.of(), revision);
        Pod desired = createPod(Map.of("app", "kafka", "version", "2.0"), Map.of(), revision);

        PodDiff diff = PodDiff.diff(current, desired);

        assertThat(diff.getChangeType(), is(PodDiff.ChangeType.METADATA_ONLY));
        assertTrue(diff.isPatchable());
        assertThat(diff.getLabelChanges(), is(Map.of("version", "2.0")));
    }

    @Test
    public void testLabelRemoved() {
        String revision = "abc123";
        Pod current = createPod(Map.of("app", "kafka", "deprecated", "true"), Map.of(), revision);
        Pod desired = createPod(Map.of("app", "kafka"), Map.of(), revision);

        PodDiff diff = PodDiff.diff(current, desired);

        assertThat(diff.getChangeType(), is(PodDiff.ChangeType.METADATA_ONLY));
        assertTrue(diff.isPatchable());
        assertThat(diff.getLabelsToRemove(), is(Map.of("deprecated", "true")));
    }

    @Test
    public void testAnnotationAdded() {
        String revision = "abc123";
        Pod current = createPod(Map.of("app", "kafka"), Map.of(), revision);
        Pod desired = createPod(Map.of("app", "kafka"), Map.of("prometheus.io/scrape", "true"), revision);

        PodDiff diff = PodDiff.diff(current, desired);

        assertThat(diff.getChangeType(), is(PodDiff.ChangeType.METADATA_ONLY));
        assertTrue(diff.isPatchable());
        assertThat(diff.getAnnotationChanges(), is(Map.of("prometheus.io/scrape", "true")));
    }

    @Test
    public void testSpecChangedRequiresRestart() {
        Pod current = createPod(Map.of("app", "kafka"), Map.of(), "revision-v1");
        Pod desired = createPod(Map.of("app", "kafka"), Map.of(), "revision-v2");

        PodDiff diff = PodDiff.diff(current, desired);

        assertThat(diff.getChangeType(), is(PodDiff.ChangeType.REQUIRES_RESTART));
        assertTrue(diff.hasChanges());
        assertFalse(diff.isPatchable());
    }

    @Test
    public void testCreatePatchPod() {
        String revision = "abc123";
        Pod current = createPod(Map.of("app", "kafka"), Map.of(), revision);
        Pod desired = createPod(Map.of("app", "kafka", "environment", "prod"), Map.of("description", "Production"), revision);

        PodDiff diff = PodDiff.diff(current, desired);
        Pod patchPod = diff.createPatchPod(current);

        assertThat(patchPod.getMetadata().getName(), is(POD_NAME));
        assertThat(patchPod.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(patchPod.getMetadata().getLabels().get("environment"), is("prod"));
        assertThat(patchPod.getMetadata().getAnnotations().get("description"), is("Production"));
    }

    @Test
    public void testCreatePatchPodThrowsForNonPatchable() {
        Pod current = createPod(Map.of("app", "kafka"), Map.of(), "revision-v1");
        Pod desired = createPod(Map.of("app", "kafka"), Map.of(), "revision-v2");

        PodDiff diff = PodDiff.diff(current, desired);

        assertThrows(IllegalStateException.class, () -> diff.createPatchPod(current));
    }

    @Test
    public void testCreatePatchPodThrowsForNullPod() {
        String revision = "abc123";
        Pod current = createPod(Map.of("app", "kafka"), Map.of(), revision);
        Pod desired = createPod(Map.of("app", "kafka", "env", "prod"), Map.of(), revision);

        PodDiff diff = PodDiff.diff(current, desired);

        assertThrows(IllegalArgumentException.class, () -> diff.createPatchPod(null));
    }

    @Test
    public void testNullPodsThrowsException() {
        Pod pod = createPod(Map.of("app", "kafka"), Map.of(), "abc123");

        assertThrows(IllegalArgumentException.class, () -> PodDiff.diff(null, pod));
        assertThrows(IllegalArgumentException.class, () -> PodDiff.diff(pod, null));
    }

    @Test
    public void testGetSummaryNoChanges() {
        String revision = "abc123";
        Pod current = createPod(Map.of("app", "kafka"), Map.of(), revision);
        Pod desired = createPod(Map.of("app", "kafka"), Map.of(), revision);

        PodDiff diff = PodDiff.diff(current, desired);

        assertThat(diff.getSummary(), is("no changes"));
    }

    @Test
    public void testGetSummaryWithChanges() {
        String revision = "abc123";
        Pod current = createPod(Map.of("app", "kafka", "old", "label"), Map.of(), revision);
        Pod desired = createPod(Map.of("app", "kafka", "new", "label"), Map.of("ann", "value"), revision);

        PodDiff diff = PodDiff.diff(current, desired);
        String summary = diff.getSummary();

        assertThat(summary, containsString("labels to update"));
        assertThat(summary, containsString("labels to remove"));
        assertThat(summary, containsString("annotations to update"));
    }

    @Test
    public void testEqualsAndHashCode() {
        String revision = "abc123";
        Pod current = createPod(Map.of("app", "kafka"), Map.of(), revision);
        Pod desired = createPod(Map.of("app", "kafka", "env", "prod"), Map.of(), revision);

        PodDiff diff1 = PodDiff.diff(current, desired);
        PodDiff diff2 = PodDiff.diff(current, desired);

        assertThat(diff1, is(diff2));
        assertThat(diff1.hashCode(), is(diff2.hashCode()));
    }

    @Test
    public void testEqualsWithDifferentChanges() {
        String revision = "abc123";
        Pod current = createPod(Map.of("app", "kafka"), Map.of(), revision);
        Pod desired1 = createPod(Map.of("app", "kafka", "env", "prod"), Map.of(), revision);
        Pod desired2 = createPod(Map.of("app", "kafka", "env", "staging"), Map.of(), revision);

        PodDiff diff1 = PodDiff.diff(current, desired1);
        PodDiff diff2 = PodDiff.diff(current, desired2);

        assertThat(diff1, is(not(diff2)));
    }

    @Test
    public void testMapsAreImmutable() {
        String revision = "abc123";
        Pod current = createPod(Map.of("app", "kafka"), Map.of(), revision);
        Pod desired = createPod(Map.of("app", "kafka", "env", "prod"), Map.of(), revision);

        PodDiff diff = PodDiff.diff(current, desired);

        assertThrows(UnsupportedOperationException.class, () -> diff.getLabelChanges().put("test", "value"));
        assertThrows(UnsupportedOperationException.class, () -> diff.getAnnotationChanges().put("test", "value"));
        assertThrows(UnsupportedOperationException.class, () -> diff.getLabelsToRemove().put("test", "value"));
        assertThrows(UnsupportedOperationException.class, () -> diff.getAnnotationsToRemove().put("test", "value"));
    }
}
