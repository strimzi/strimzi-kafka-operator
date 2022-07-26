/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.platform;

/**
 * Interface which describes the Kubernetes platform on which we run
 */
public interface PlatformFeatures {
    /**
     * @return Returns the Kubernetes version on which Strimzi is running.
     */
    KubernetesVersion getKubernetesVersion();

    /**
     * @return Returns true if the cluster Strimzi is running on was identified as OpenShift and false otherwise.
     */
    boolean isOpenshift();
}
