package io.strimzi.controller.cluster.resources;

public class ClusterDiffResult {
    private boolean different = false;
    private boolean rollingUpdate = false;
    private boolean scaleUp = false;
    private boolean scaleDown = false;
    private boolean isMetricsChanged = false;

    public ClusterDiffResult() {
        // Nothing to do
    }

    public ClusterDiffResult(boolean isDifferent) {
        this.different = isDifferent;
    }

    public ClusterDiffResult(boolean isDifferent, boolean needsRollingUpdate) {
        this.different = isDifferent;
        this.rollingUpdate = needsRollingUpdate;
    }

    public ClusterDiffResult(boolean isDifferent, boolean needsRollingUpdate, boolean isScaleUp, boolean isScaleDown) {
        this.different = isDifferent;
        this.rollingUpdate = needsRollingUpdate;
        this.scaleUp = isScaleUp;
        this.scaleDown = isScaleDown;
    }

    /**
     * Determines whether a resource needs to be updated/patched.
     * This doesn't distinguish which resource, but patching unnecessarily ends up being a no-op.
     * @return true iff a resource needs to be updated/patched
     */
    public boolean getDifferent() {
        return different;
    }

    public void setDifferent(boolean different) {
        this.different = different;
    }

    public boolean getRollingUpdate() {
        return rollingUpdate;
    }

    public void setRollingUpdate(boolean rollingUpdate) {
        setDifferent(true);
        this.rollingUpdate = rollingUpdate;
    }

    public boolean getScaleUp() {
        return scaleUp;
    }

    public void setScaleUp(boolean scaleUp) {
        this.scaleUp = scaleUp;
    }

    public boolean getScaleDown() {
        return scaleDown;
    }

    public void setScaleDown(boolean scaleDown) {
        this.scaleDown = scaleDown;
    }

    public boolean isMetricsChanged() {
        return isMetricsChanged;
    }

    public void setMetricsChanged(boolean metricsChanged) {
        isMetricsChanged = metricsChanged;
    }
}
