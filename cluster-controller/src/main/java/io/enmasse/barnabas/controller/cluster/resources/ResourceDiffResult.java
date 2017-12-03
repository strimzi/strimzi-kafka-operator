package io.enmasse.barnabas.controller.cluster.resources;

public class ResourceDiffResult {
    private Boolean different = false;
    private Boolean rollingUpdate = false;
    private Boolean scaleUp = false;
    private Boolean scaleDown = false;

    public ResourceDiffResult() {
        // Nothing to do
    }

    public ResourceDiffResult(Boolean isDifferent) {
        this.different = isDifferent;
    }

    public ResourceDiffResult(Boolean isDifferent, Boolean needsRollingUpdate) {
        this.different = isDifferent;
        this.rollingUpdate = needsRollingUpdate;
    }

    public ResourceDiffResult(Boolean isDifferent, Boolean needsRollingUpdate, Boolean isScaleUp, Boolean isScaleDown) {
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
        setDifferent(true);
        this.scaleUp = scaleUp;
    }

    public Boolean getScaleDown() {
        return scaleDown;
    }

    public void setScaleDown(Boolean scaleDown) {
        setDifferent(true);
        this.scaleDown = scaleDown;
    }
}
