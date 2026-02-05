/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Utility class for computing differences between pods and determining if changes
 * can be applied in-place (patched) without requiring a pod restart.
 *
 * <p>In Kubernetes, most pod spec fields are immutable after creation. However,
 * certain metadata fields (labels and annotations) can be modified on a running pod
 * without triggering a restart.</p>
 *
 * <p>This class is immutable and thread-safe once constructed.</p>
 */
public final class PodDiff {

    /**
     * Enum representing the type of change detected between two pods.
     */
    public enum ChangeType {
        /** No changes detected */
        NONE,
        /** Only metadata (labels/annotations) changed - can be patched in-place */
        METADATA_ONLY,
        /** Spec or other immutable fields changed - requires pod restart */
        REQUIRES_RESTART
    }

    /** Singleton instance representing no changes */
    private static final PodDiff NO_CHANGE = new PodDiff(
            ChangeType.NONE, 
            Collections.emptyMap(), 
            Collections.emptyMap(), 
            Collections.emptyMap(), 
            Collections.emptyMap()
    );

    private final ChangeType changeType;
    private final Map<String, String> labelChanges;
    private final Map<String, String> annotationChanges;
    private final Map<String, String> labelsToRemove;
    private final Map<String, String> annotationsToRemove;

    /**
     * Private constructor. Use {@link #diff(Pod, Pod)} to create instances.
     *
     * @param changeType          The type of change detected
     * @param labelChanges        Labels to add or update
     * @param annotationChanges   Annotations to add or update
     * @param labelsToRemove      Labels to remove
     * @param annotationsToRemove Annotations to remove
     */
    private PodDiff(ChangeType changeType,
                    Map<String, String> labelChanges,
                    Map<String, String> annotationChanges,
                    Map<String, String> labelsToRemove,
                    Map<String, String> annotationsToRemove) {
        this.changeType = changeType;
        // Store immutable copies to ensure thread-safety
        this.labelChanges = Collections.unmodifiableMap(new HashMap<>(labelChanges));
        this.annotationChanges = Collections.unmodifiableMap(new HashMap<>(annotationChanges));
        this.labelsToRemove = Collections.unmodifiableMap(new HashMap<>(labelsToRemove));
        this.annotationsToRemove = Collections.unmodifiableMap(new HashMap<>(annotationsToRemove));
    }

    /**
     * Computes the difference between the current pod and the desired pod.
     *
     * @param current The current pod state (from the cluster)
     * @param desired The desired pod state (from the StrimziPodSet)
     * @return A PodDiff describing the changes
     * @throws IllegalArgumentException if either pod is null
     */
    public static PodDiff diff(Pod current, Pod desired) {
        if (current == null || desired == null) {
            throw new IllegalArgumentException("Current and desired pods must not be null");
        }

        boolean specChanged = PodRevision.hasChanged(current, desired);

        Map<String, String> currentLabels = nullSafeMap(current.getMetadata().getLabels());
        Map<String, String> desiredLabels = nullSafeMap(desired.getMetadata().getLabels());
        Map<String, String> currentAnnotations = nullSafeMap(current.getMetadata().getAnnotations());
        Map<String, String> desiredAnnotations = nullSafeMap(desired.getMetadata().getAnnotations());

        Map<String, String> labelChanges = computeAdditions(currentLabels, desiredLabels);
        Map<String, String> labelsToRemove = computeRemovals(currentLabels, desiredLabels);
        Map<String, String> annotationChanges = computeAdditions(currentAnnotations, desiredAnnotations);
        Map<String, String> annotationsToRemove = computeRemovals(currentAnnotations, desiredAnnotations);

        boolean hasMetadataChanges = !labelChanges.isEmpty() || !annotationChanges.isEmpty() 
                || !labelsToRemove.isEmpty() || !annotationsToRemove.isEmpty();

        // Return singleton for no-change case to reduce allocations
        if (!specChanged && !hasMetadataChanges) {
            return NO_CHANGE;
        }

        ChangeType changeType = determineChangeType(specChanged, hasMetadataChanges);

        return new PodDiff(changeType, labelChanges, annotationChanges, labelsToRemove, annotationsToRemove);
    }

    /**
     * Gets a non-null map, returning empty map if input is null.
     *
     * @param map The input map (may be null)
     * @return The input map or an empty immutable map if null
     */
    private static Map<String, String> nullSafeMap(Map<String, String> map) {
        return map != null ? map : Collections.emptyMap();
    }

    /**
     * Computes entries that need to be added or updated.
     *
     * @param current The current map state
     * @param desired The desired map state
     * @return Map containing entries to add or update
     */
    private static Map<String, String> computeAdditions(Map<String, String> current, Map<String, String> desired) {
        Map<String, String> changes = new HashMap<>();
        for (Map.Entry<String, String> entry : desired.entrySet()) {
            if (!Objects.equals(current.get(entry.getKey()), entry.getValue())) {
                changes.put(entry.getKey(), entry.getValue());
            }
        }
        return changes;
    }

    /**
     * Computes entries that need to be removed.
     *
     * @param current The current map state
     * @param desired The desired map state
     * @return Map containing entries to remove
     */
    private static Map<String, String> computeRemovals(Map<String, String> current, Map<String, String> desired) {
        Map<String, String> removals = new HashMap<>();
        for (Map.Entry<String, String> entry : current.entrySet()) {
            if (!desired.containsKey(entry.getKey())) {
                removals.put(entry.getKey(), entry.getValue());
            }
        }
        return removals;
    }

    /**
     * Determines the change type based on spec and metadata changes.
     *
     * @param specChanged        Whether the pod spec has changed
     * @param hasMetadataChanges Whether there are metadata changes
     * @return The appropriate ChangeType
     */
    private static ChangeType determineChangeType(boolean specChanged, boolean hasMetadataChanges) {
        if (specChanged) {
            return ChangeType.REQUIRES_RESTART;
        } else if (hasMetadataChanges) {
            return ChangeType.METADATA_ONLY;
        } else {
            return ChangeType.NONE;
        }
    }

    /**
     * Returns the type of change detected.
     *
     * @return The change type
     */
    public ChangeType getChangeType() {
        return changeType;
    }

    /**
     * Checks if the changes can be applied via an in-place patch (no restart required).
     *
     * @return true if only patchable (metadata) changes were detected
     */
    public boolean isPatchable() {
        return changeType == ChangeType.METADATA_ONLY;
    }

    /**
     * Checks if there are any changes at all.
     *
     * @return true if changes were detected
     */
    public boolean hasChanges() {
        return changeType != ChangeType.NONE;
    }

    /**
     * Returns the total count of metadata changes (additions, updates, and removals).
     *
     * @return The total number of metadata changes
     */
    public int getTotalChangeCount() {
        return labelChanges.size() + annotationChanges.size() 
                + labelsToRemove.size() + annotationsToRemove.size();
    }

    /**
     * Returns the labels that need to be added or updated.
     *
     * @return Immutable map of label changes
     */
    public Map<String, String> getLabelChanges() {
        return labelChanges;
    }

    /**
     * Returns the annotations that need to be added or updated.
     *
     * @return Immutable map of annotation changes
     */
    public Map<String, String> getAnnotationChanges() {
        return annotationChanges;
    }

    /**
     * Returns the labels that should be removed.
     *
     * @return Immutable map of labels to remove
     */
    public Map<String, String> getLabelsToRemove() {
        return labelsToRemove;
    }

    /**
     * Returns the annotations that should be removed.
     *
     * @return Immutable map of annotations to remove
     */
    public Map<String, String> getAnnotationsToRemove() {
        return annotationsToRemove;
    }

    /**
     * Creates a minimal Pod object suitable for patching the current pod's metadata.
     * This creates a pod with only the essential fields for a strategic merge patch.
     *
     * @param currentPod The current pod to base the patch on
     * @return A Pod object containing only the metadata changes
     * @throws IllegalStateException if this diff is not patchable
     * @throws IllegalArgumentException if currentPod is null
     */
    public Pod createPatchPod(Pod currentPod) {
        if (!isPatchable()) {
            throw new IllegalStateException("Cannot create patch pod for non-patchable changes (changeType=" + changeType + ")");
        }
        if (currentPod == null) {
            throw new IllegalArgumentException("Current pod must not be null");
        }

        Map<String, String> newLabels = new HashMap<>(nullSafeMap(currentPod.getMetadata().getLabels()));
        Map<String, String> newAnnotations = new HashMap<>(nullSafeMap(currentPod.getMetadata().getAnnotations()));

        // Apply additions/updates
        newLabels.putAll(labelChanges);
        newAnnotations.putAll(annotationChanges);

        // Apply removals
        labelsToRemove.keySet().forEach(newLabels::remove);
        annotationsToRemove.keySet().forEach(newAnnotations::remove);

        ObjectMeta patchMeta = new ObjectMetaBuilder()
                .withName(currentPod.getMetadata().getName())
                .withNamespace(currentPod.getMetadata().getNamespace())
                .withLabels(newLabels.isEmpty() ? null : newLabels)
                .withAnnotations(newAnnotations.isEmpty() ? null : newAnnotations)
                .build();

        return new PodBuilder()
                .withMetadata(patchMeta)
                .build();
    }

    /**
     * Returns a human-readable summary of the changes for logging purposes.
     *
     * @return A summary string describing the changes
     */
    public String getSummary() {
        if (!hasChanges()) {
            return "no changes";
        }

        StringBuilder sb = new StringBuilder();
        if (!labelChanges.isEmpty()) {
            sb.append("labels to update: ").append(labelChanges.keySet());
        }
        if (!labelsToRemove.isEmpty()) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append("labels to remove: ").append(labelsToRemove.keySet());
        }
        if (!annotationChanges.isEmpty()) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append("annotations to update: ").append(annotationChanges.keySet());
        }
        if (!annotationsToRemove.isEmpty()) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append("annotations to remove: ").append(annotationsToRemove.keySet());
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PodDiff podDiff = (PodDiff) o;
        return changeType == podDiff.changeType 
                && Objects.equals(labelChanges, podDiff.labelChanges) 
                && Objects.equals(annotationChanges, podDiff.annotationChanges) 
                && Objects.equals(labelsToRemove, podDiff.labelsToRemove) 
                && Objects.equals(annotationsToRemove, podDiff.annotationsToRemove);
    }

    @Override
    public int hashCode() {
        return Objects.hash(changeType, labelChanges, annotationChanges, labelsToRemove, annotationsToRemove);
    }

    @Override
    public String toString() {
        return "PodDiff{" +
                "changeType=" + changeType +
                ", labelChanges=" + labelChanges.size() +
                ", annotationChanges=" + annotationChanges.size() +
                ", labelsToRemove=" + labelsToRemove.size() +
                ", annotationsToRemove=" + annotationsToRemove.size() +
                '}';
    }
}
