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
    public static final KubernetesVersion V1_8 = new KubernetesVersion(1, 8);

    // supported versions
    public static final KubernetesVersion V1_9 = new KubernetesVersion(1, 9);
    public static final KubernetesVersion V1_10 = new KubernetesVersion(1, 10);
    public static final KubernetesVersion V1_11 = new KubernetesVersion(1, 11);
    public static final KubernetesVersion V1_12 = new KubernetesVersion(1, 12);
    public static final KubernetesVersion V1_13 = new KubernetesVersion(1, 13);
    public static final KubernetesVersion V1_14 = new KubernetesVersion(1, 14);
    public static final KubernetesVersion V1_16 = new KubernetesVersion(1, 16);


    public static final KubernetesVersion MINIMAL_SUPPORTED_VERSION = V1_9;
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

