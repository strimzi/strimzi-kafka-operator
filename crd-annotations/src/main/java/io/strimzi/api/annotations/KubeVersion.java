/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.annotations;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Short.parseShort;

/**
 * Represents a Kubernetes version number and embeds some knowledge of the features available in
 * different Kubernetes versions.
 */
public class KubeVersion implements Comparable<KubeVersion> {
    /**
     * Kubernetes 1.11.x
     */
    public static final KubeVersion V1_11 = new KubeVersion((short) 1, (short) 11);

    /**
     * Kubernetes 1.16.x
     */
    public static final KubeVersion V1_16 = new KubeVersion((short) 1, (short) 16);

    /**
     * Kubernetes range for Kube 1.11 and newer
     */
    public static final VersionRange<KubeVersion> V1_11_PLUS = KubeVersion.parseRange("1.11+");

    /**
     * Kubernetes range for Kube 1.16 and newer
     */
    public static final VersionRange<KubeVersion> V1_16_PLUS = KubeVersion.parseRange("1.16+");

    private final short major;
    private final short minor;

    private KubeVersion(short major, short minor) {
        this.major = major;
        this.minor = minor;
    }

    /**
     * Parses Kubernetes version from String to KubeVersion instance
     *
     * @param version   Kubernetes version string which should be parsed
     *
     * @return  KubeVersion instance
     */
    public static KubeVersion parse(String version) {
        Pattern pattern = Pattern.compile("([0-9]+).([0-9]+)");
        Matcher matcher = pattern.matcher(version);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid kube version");
        }
        return new KubeVersion(parseShort(matcher.group(1)), parseShort(matcher.group(2)));
    }

    /**
     * Parses range of Kubernetes versions
     *
     * @param range     String with the Kubernetes versions range
     *
     * @return  Instance of the VersionRange class
     */
    public static VersionRange<KubeVersion> parseRange(String range) {
        return VersionRange.parse(range, KubeVersion::parse);
    }

    @Override
    public int compareTo(KubeVersion o) {
        int cmp = Short.compare(major, o.major);
        if (cmp == 0) {
            cmp = Short.compare(minor, o.minor);
        }
        return cmp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KubeVersion that = (KubeVersion) o;
        return major == that.major &&
                minor == that.minor;
    }

    @Override
    public int hashCode() {
        return Objects.hash(major, minor);
    }

    @Override
    public String toString() {
        return major + "." + minor;
    }

    /**
     * Indicates whether this Kubernetes version support CRDs with schema per version
     *
     * @return  True if schema-oer version is supported in CRDs in this Kubernetes versions. False otherwise.
     */
    public boolean supportsSchemaPerVersion() {
        return this.compareTo(V1_16) >= 0;
    }

    /**
     * Checks whether all Kubernetes versions in this version range supports schemas per version in CRDs
     *
     * @param versionRange  The version range which should be checked
     *
     * @return  True if all Kubernetes versions in the range support schemas per version in the CRDs. False otherwise.
     */
    public static boolean supportsSchemaPerVersion(VersionRange<KubeVersion> versionRange) {
        if (versionRange.isEmpty() || versionRange.isAll()) {
            return false;
        } else {
            return versionRange.lower().supportsSchemaPerVersion()
                    && (versionRange.upper() == null
                    || versionRange.upper().supportsSchemaPerVersion());
        }
    }

    /**
     * Does this version of Kubernetes support the given API version of CustomResourceDefinition?
     *
     * @param crdApiVersion The CustomResourceDefinition API version
     *
     * @return Whether this version of Kube has support
     */
    public boolean supportsCrdApiVersion(ApiVersion crdApiVersion) {
        return crdApiVersion.equals(ApiVersion.V1) ? this.compareTo(V1_16) >= 0 : this.compareTo(V1_11) >= 0;
    }
}
