/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.strimzi.operator.cluster.operator.KubernetesVersion;


public class PlatformFeaturesAvailability {

    private boolean isOpenshift;
    private final KubernetesVersion kubernetesVersion;

    public PlatformFeaturesAvailability(boolean isOpenshift, KubernetesVersion kubernetesVersion) {
        this.isOpenshift = isOpenshift;
        this.kubernetesVersion = kubernetesVersion;
    }

    public boolean isOpenshift() {
        return this.isOpenshift;
    }

    public int getMinorVersion() {
        return kubernetesVersion.getMinor();
    }

    public int getMajorVersion() {
        return kubernetesVersion.getMajor();
    }

    public boolean isNetworkPolicyPodSelectorAndNameSpaceInSinglePeerAvailable() {
        return this.kubernetesVersion.compareTo(KubernetesVersion.V1_11) >= 0;
    }
}
