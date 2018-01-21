package io.strimzi.controller.cluster.resources;

public class ClusterDiffResult {
    private Boolean different = false;
    private Boolean rollingUpdate = false;
    private Boolean scaleUp = false;
    private Boolean scaleDown = false;
    private boolean isMetricsChanged = false;
    private Source2Image.Source2ImageDiff s2i = Source2Image.Source2ImageDiff.NONE;

    public ClusterDiffResult() {
        // Nothing to do
    }

    public ClusterDiffResult(Boolean isDifferent) {
        this.different = isDifferent;
    }

    public ClusterDiffResult(Boolean isDifferent, Boolean needsRollingUpdate) {
        this.different = isDifferent;
        this.rollingUpdate = needsRollingUpdate;
    }

    public ClusterDiffResult(Boolean isDifferent, Boolean needsRollingUpdate, Boolean isScaleUp, Boolean isScaleDown) {
        this.different = isDifferent;
        this.rollingUpdate = needsRollingUpdate;
        this.scaleUp = isScaleUp;
        this.scaleDown = isScaleDown;
    }

    public Boolean getDifferent() {
        return different;
    }

    public void setDifferent(Boolean different) {
        this.different = different;
    }

    public Boolean getRollingUpdate() {
        return rollingUpdate;
    }

    public void setRollingUpdate(Boolean rollingUpdate) {
        setDifferent(true);
        this.rollingUpdate = rollingUpdate;
    }

    public Boolean getScaleUp() {
        return scaleUp;
    }

    public void setScaleUp(Boolean scaleUp) {
        this.scaleUp = scaleUp;
    }

    public Boolean getScaleDown() {
        return scaleDown;
    }

    public void setScaleDown(Boolean scaleDown) {
        this.scaleDown = scaleDown;
    }

    public boolean isMetricsChanged() {
        return isMetricsChanged;
    }

    public void setMetricsChanged(boolean metricsChanged) {
        isMetricsChanged = metricsChanged;
    }

    public Source2Image.Source2ImageDiff getS2i() {
        return s2i;
    }

    public void setS2i(Source2Image.Source2ImageDiff s2i) {
        this.s2i = s2i;
    }
}
