/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator;

public class KubernetesVersion implements Comparable<KubernetesVersion> {

    private int major;
    private int minor;
    private String version;
    public static final KubernetesVersion V1_8 = new KubernetesVersion("1", "8");
    public static final KubernetesVersion V1_9 = new KubernetesVersion("1", "9");
    public static final KubernetesVersion V1_10 = new KubernetesVersion("1", "10");
    public static final KubernetesVersion V1_11 = new KubernetesVersion("1", "11");
    public static final KubernetesVersion V1_12 = new KubernetesVersion("1", "12");

    public KubernetesVersion(String major, String minor) {
        this.major = Integer.parseInt(major.replaceAll("\\D", ""));
        this.minor = Integer.parseInt(minor.replaceAll("\\D", ""));
        this.version = major + "." + minor;
    }

    @Override
    public int hashCode() {
        return version.hashCode();
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
        int multiplier = 10000; //reasonably large number
        return (major * multiplier + minor) - (o.major * multiplier + o.minor);
    }

    public int getMajor() {
        return major;
    }

    public int getMinor() {
        return minor;
    }
}

