package io.strimzi.controller.cluster.resources;

/**
 * Describes the difference between two cluster states in terms of the kind of actions necessary
 * to make them consistent.
 */
public class ClusterDiffResult {
    private final boolean different;
    private final boolean rollingUpdate;
    private final boolean scaleUp;
    private final boolean scaleDown;
    private final boolean isMetricsChanged;

    public ClusterDiffResult() {
        this(false);
    }

    public ClusterDiffResult(boolean isDifferent) {
        this(isDifferent, false, false, false, false);
    }

    public ClusterDiffResult(boolean isDifferent, boolean needsRollingUpdate) {
        this(isDifferent, needsRollingUpdate, false, false, false);
    }

    public ClusterDiffResult(boolean isDifferent, boolean needsRollingUpdate, boolean isScaleUp, boolean isScaleDown, boolean isMetricsChanged) {
        this.different = isDifferent || needsRollingUpdate;
        this.rollingUpdate = needsRollingUpdate;
        this.scaleUp = isScaleUp;
        this.scaleDown = isScaleDown;
        this.isMetricsChanged = isMetricsChanged;
    }

    /**
     * Determines whether a resource needs to be updated/patched.
     * This doesn't distinguish which resource, but patching unnecessarily ends up being a no-op.
     * @return true iff a resource needs to be updated/patched
     */
    public boolean getDifferent() {
        return different;
    }

    public boolean isRollingUpdate() {
        return rollingUpdate;
    }

    public boolean isScaleUp() {
        return scaleUp;
    }

    public boolean isScaleDown() {
        return scaleDown;
    }

    public boolean isMetricsChanged() {
        return isMetricsChanged;
    }
}
