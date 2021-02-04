/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator;

/**
 * Represents Kubernetes version which CO runs on
 */
public class KubernetesVersion implements Comparable<KubernetesVersion> {

    private int major;
    private int minor;

    // unsupported versions
    public static final KubernetesVersion V1_15 = new KubernetesVersion(1, 15);

    // supported versions
    public static final KubernetesVersion V1_16 = new KubernetesVersion(1, 16);
    public static final KubernetesVersion V1_17 = new KubernetesVersion(1, 17);
    public static final KubernetesVersion V1_18 = new KubernetesVersion(1, 18);
    public static final KubernetesVersion V1_19 = new KubernetesVersion(1, 19);
    public static final KubernetesVersion V1_20 = new KubernetesVersion(1, 20);

    public static final KubernetesVersion MINIMAL_SUPPORTED_VERSION = V1_16;
    public static final int MINIMAL_SUPPORTED_MAJOR = MINIMAL_SUPPORTED_VERSION.major;
    public static final int MINIMAL_SUPPORTED_MINOR = MINIMAL_SUPPORTED_VERSION.minor;

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

