/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.strimzi.operator.cluster.operator.KubernetesVersion;

/**
 * Gives a info about certain features availability regarding to kubernetes version
 */
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

    public boolean isNamespaceAndPodSelectorNetworkPolicySupported() {
        return this.kubernetesVersion.compareTo(KubernetesVersion.V1_11) >= 0;
    }

    public KubernetesVersion getKubernetesVersion() {
        return this.kubernetesVersion;
    }
}
