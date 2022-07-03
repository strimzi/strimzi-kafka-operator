/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.platform;

public interface PlatformFeatures {
    /**
     * @return Returns the Kubernetes version on which the operator is running.
     */
    KubernetesVersion getKubernetesVersion();

    /**
     * @return Returns true if the cluster was identified as OpenShift and false otherwise.
     */
    boolean isOpenshift();
}
