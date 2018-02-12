/*
 * Copyright 2018, Strimzi Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    public boolean isDifferent() {
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
