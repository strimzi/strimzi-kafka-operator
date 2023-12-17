/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.platform;

/**
 * Represents Kubernetes version which Strimzi runs on
 */
public class KubernetesVersion implements Comparable<KubernetesVersion> {
    private final int major;
    private final int minor;

    // Notable Kubernetes versions => this includes the minimal supported version and for example also any Kubernetes
    // versions from some features are supported.
    public static final KubernetesVersion V1_23 = new KubernetesVersion(1, 23);

    public static final KubernetesVersion MINIMAL_SUPPORTED_VERSION = V1_23;
    public static final int MINIMAL_SUPPORTED_MAJOR = MINIMAL_SUPPORTED_VERSION.major;
    public static final int MINIMAL_SUPPORTED_MINOR = MINIMAL_SUPPORTED_VERSION.minor;

    /**
     * Constructs the Kubernetes version from major and minor version
     *
     * @param major     Major Kubernetes version
     * @param minor     Minor Kubernetes version
     */
    public KubernetesVersion(int major, int minor) {
        this.major = major;
        this.minor = minor;
    }

    @Override
    public int hashCode() {
        return major << 16 ^ minor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KubernetesVersion that = (KubernetesVersion) o;
        return major == that.major && minor == that.minor;
    }

    @Override
    public int compareTo(KubernetesVersion o) {
        int cmp = Integer.compare(major, o.major);
        if (cmp == 0) {
            cmp = Integer.compare(minor, o.minor);
        }
        return cmp;
    }

    @Override
    public String toString() {
        return this.major + "." + this.minor;
    }
}

